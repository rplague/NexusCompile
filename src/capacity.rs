//! Local capacity sampling and slot calculation.

use std::fs;
use std::io;

use crate::config::NodeConfig;

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

/// Calculate stable total capacity and current free capacity.
pub fn calculate_capacity(
    sample: &CapacitySample,
    active_slots: u32,
    queue_len: u32,
    config: &NodeConfig,
) -> CapacitySnapshot {
    let cores_per_slot = config.cores_per_slot.max(0.01);
    let mem_per_slot = config.mem_per_slot_mb.max(1);
    let total_cores = sample.total_cores.max(1.0);

    let total_cpu_slots = ((total_cores - config.reserve_cores).max(0.0) / cores_per_slot)
        .floor()
        .min(u32::MAX as f32) as u32;
    let total_mem_slots = (sample.mem_total_mb.saturating_sub(config.reserve_mem_mb) / mem_per_slot)
        .min(u32::MAX as u64) as u32;
    let max_slots = total_cpu_slots.min(total_mem_slots);

    let free_cpu_slots = ((total_cores - sample.used_cores - config.reserve_cores).max(0.0)
        / cores_per_slot)
        .floor()
        .min(u32::MAX as f32) as u32;
    let free_mem_slots = (sample
        .mem_available_mb
        .saturating_sub(config.reserve_mem_mb)
        / mem_per_slot)
        .min(u32::MAX as u64) as u32;
    let currently_available = free_cpu_slots.min(free_mem_slots);
    let free_slots = max_slots
        .saturating_sub(active_slots)
        .min(currently_available);

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

    fn config() -> NodeConfig {
        NodeConfig {
            cores_per_slot: 1.0,
            mem_per_slot_mb: 2048,
            reserve_cores: 0.5,
            reserve_mem_mb: 1024,
            ..NodeConfig::default()
        }
    }

    #[test]
    fn computes_stable_max_and_dynamic_free() {
        let sample = CapacitySample {
            total_cores: 4.0,
            used_cores: 1.2,
            mem_total_mb: 8192,
            mem_available_mb: 4096,
        };
        let snapshot = calculate_capacity(&sample, 0, 0, &config());
        assert_eq!(snapshot.max_slots, 3);
        assert_eq!(snapshot.free_slots, 1);
        assert!(snapshot.accepting);
    }

    #[test]
    fn active_slots_reduce_free_slots() {
        let sample = CapacitySample {
            total_cores: 16.0,
            used_cores: 0.0,
            mem_total_mb: 16384,
            mem_available_mb: 16384,
        };
        let snapshot = calculate_capacity(&sample, 2, 0, &config());
        assert_eq!(snapshot.max_slots, 7);
        assert_eq!(snapshot.free_slots, 5);
    }

    #[test]
    fn full_memory_disables_accepting() {
        let sample = CapacitySample {
            total_cores: 8.0,
            used_cores: 0.0,
            mem_total_mb: 8192,
            mem_available_mb: 1024,
        };
        let snapshot = calculate_capacity(&sample, 0, 0, &config());
        assert_eq!(snapshot.free_slots, 0);
        assert!(!snapshot.accepting);
    }

    #[test]
    fn zero_capacity_is_not_accepting() {
        let sample = CapacitySample {
            total_cores: 1.0,
            used_cores: 1.0,
            mem_total_mb: 1024,
            mem_available_mb: 0,
        };
        let snapshot = calculate_capacity(&sample, 0, 0, &config());
        assert_eq!(snapshot.max_slots, 0);
        assert_eq!(snapshot.free_slots, 0);
        assert!(!snapshot.accepting);
    }
}
