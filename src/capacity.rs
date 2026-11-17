//! Local capacity sampling and slot calculation.

use std::fs;
use std::io;

/// Fixed resources consumed by one compile slot.
pub const CORES_PER_SLOT: f32 = 1.0;
pub const MEM_PER_SLOT_MB: u64 = 2048;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CapacitySample {
    pub total_cores: f32,
    pub used_cores: f32,
    pub mem_total_mb: u64,
    pub mem_available_mb: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacitySnapshot {
    pub max_slots: u32,
    pub free_slots: u32,
    pub queue_len: u32,
    pub accepting: bool,
}

pub trait CapacitySource: Send {
    fn sample(&mut self) -> io::Result<CapacitySample>;
}

/// Linux `/proc/stat` + `/proc/meminfo` sampler.
pub struct LinuxProcSource {
    total_cores: f32,
    prev_total: u64,
    prev_idle: u64,
    used_ewma: Option<f32>,
}

impl LinuxProcSource {
    pub fn new() -> io::Result<Self> {
        let (prev_total, prev_idle) = read_proc_stat()?;
        // Read meminfo once so an unreadable file fails at startup rather than
        // silently reporting zero memory on every tick.
        let _ = read_meminfo()?;
        let total_cores = std::thread::available_parallelism()
            .map(|n| n.get() as f32)
            .unwrap_or(1.0);
        Ok(Self {
            total_cores,
            prev_total,
            prev_idle,
            used_ewma: None,
        })
    }
}

impl CapacitySource for LinuxProcSource {
    fn sample(&mut self) -> io::Result<CapacitySample> {
        let (total, idle) = read_proc_stat()?;
        let (mem_total_mb, mem_available_mb) = read_meminfo()?;

        let delta_total = total.saturating_sub(self.prev_total);
        let delta_idle = idle.saturating_sub(self.prev_idle);
        let instant_used = if delta_total == 0 {
            0.0
        } else {
            let busy = delta_total.saturating_sub(delta_idle) as f32;
            (busy / delta_total as f32) * self.total_cores
        };
        let used_cores = match self.used_ewma {
            Some(previous) => 0.3 * instant_used + 0.7 * previous,
            None => instant_used,
        };
        self.used_ewma = Some(used_cores);

        self.prev_total = total;
        self.prev_idle = idle;

        Ok(CapacitySample {
            total_cores: self.total_cores,
            used_cores,
            mem_total_mb,
            mem_available_mb,
        })
    }
}

/// Fixed sample source for tests and for platforms without `/proc`.
pub struct FixedSource {
    sample: CapacitySample,
}

impl FixedSource {
    pub fn new(sample: CapacitySample) -> Self {
        Self { sample }
    }
}

impl CapacitySource for FixedSource {
    fn sample(&mut self) -> io::Result<CapacitySample> {
        Ok(self.sample)
    }
}

/// Calculate stable total capacity and current free capacity from a usage cap.
///
/// `free_slots` is resource headroom under `max_usage_percent`; it deliberately
/// does not subtract active tasks because `/proc` usage already includes them.
pub fn calculate_capacity(
    sample: &CapacitySample,
    queue_len: u32,
    max_usage_percent: u8,
) -> CapacitySnapshot {
    let percent = max_usage_percent.min(100) as f32 / 100.0;
    let total_cores = sample.total_cores.max(1.0);

    let max_cpu_cores = total_cores * percent;
    let max_mem_mb = sample.mem_total_mb as f32 * percent;
    let max_cpu_slots = (max_cpu_cores / CORES_PER_SLOT)
        .floor()
        .min(u32::MAX as f32) as u32;
    let max_mem_slots = (max_mem_mb / MEM_PER_SLOT_MB as f32)
        .floor()
        .min(u32::MAX as f32) as u32;
    let max_slots = max_cpu_slots.min(max_mem_slots);

    if max_slots == 0 {
        return CapacitySnapshot {
            max_slots: 0,
            free_slots: 0,
            queue_len,
            accepting: false,
        };
    }

    let used_mem_mb = sample.mem_total_mb.saturating_sub(sample.mem_available_mb) as f32;
    let free_cpu_slots = ((max_cpu_cores - sample.used_cores).max(0.0) / CORES_PER_SLOT)
        .floor()
        .min(u32::MAX as f32) as u32;
    let free_mem_slots = ((max_mem_mb - used_mem_mb).max(0.0) / MEM_PER_SLOT_MB as f32)
        .floor()
        .min(u32::MAX as f32) as u32;
    let free_slots = free_cpu_slots.min(free_mem_slots).min(max_slots);

    CapacitySnapshot {
        max_slots,
        free_slots,
        queue_len,
        accepting: free_slots > 0,
    }
}

fn read_proc_stat() -> io::Result<(u64, u64)> {
    let content = fs::read_to_string("/proc/stat")?;
    let line = content
        .lines()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing cpu line"))?;
    let mut fields = line.split_whitespace();
    if fields.next() != Some("cpu") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected /proc/stat format",
        ));
    }

    let mut values = Vec::new();
    for field in fields {
        match field.parse::<u64>() {
            Ok(value) => values.push(value),
            Err(_) => break,
        }
    }
    if values.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "incomplete /proc/stat cpu line",
        ));
    }

    let total: u64 = values.iter().copied().sum();
    let idle = values[3] + values.get(4).copied().unwrap_or(0);
    Ok((total, idle))
}

fn read_meminfo() -> io::Result<(u64, u64)> {
    let content = fs::read_to_string("/proc/meminfo")?;
    let mut total_kb = None;
    let mut available_kb = None;

    for line in content.lines() {
        let Some((key, rest)) = line.split_once(':') else {
            continue;
        };
        let value = rest
            .split_whitespace()
            .next()
            .and_then(|value| value.parse::<u64>().ok());
        match key {
            "MemTotal" => total_kb = value,
            "MemAvailable" => available_kb = value,
            _ => {}
        }
    }

    let total_kb =
        total_kb.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing MemTotal"))?;
    let available_kb = available_kb
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing MemAvailable"))?;

    Ok((total_kb / 1024, available_kb / 1024))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_table() {
        let cases = [
            ("usage_percent", 4.0, 1.2, 8192, 4096, 80, 3, 1, true),
            ("idle", 16.0, 0.0, 16384, 16384, 80, 6, 6, true),
            ("memory_limit", 8.0, 0.0, 8192, 1024, 80, 0, 0, false),
            ("zero_capacity", 1.0, 1.0, 1024, 0, 80, 0, 0, false),
            ("fifty_percent", 8.0, 4.0, 32768, 16384, 100, 8, 4, true),
        ];

        for (
            name,
            total_cores,
            used_cores,
            mem_total_mb,
            mem_available_mb,
            percent,
            max,
            free,
            accepting,
        ) in cases
        {
            let sample = CapacitySample {
                total_cores,
                used_cores,
                mem_total_mb,
                mem_available_mb,
            };
            let snapshot = calculate_capacity(&sample, 0, percent);
            assert_eq!(snapshot.max_slots, max, "case: {name}");
            assert_eq!(snapshot.free_slots, free, "case: {name}");
            assert_eq!(snapshot.accepting, accepting, "case: {name}");
        }

        let sample = CapacitySample {
            total_cores: 4.0,
            used_cores: 0.0,
            mem_total_mb: 16384,
            mem_available_mb: 16384,
        };
        assert_eq!(
            calculate_capacity(&sample, 0, 120),
            calculate_capacity(&sample, 0, 100),
            "case: percent clamp"
        );
    }
}
