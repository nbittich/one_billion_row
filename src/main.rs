use itertools::Itertools;
use memmap2::{Advice, MmapOptions};
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use std::{collections::HashMap, fs::File, thread::JoinHandle};

#[derive(Default, Clone, Debug)]
struct Measurement {
    min: i32,
    max: i32,
    tot: i64,
    count: u32,
}

fn main() {
    unsafe {
        run();
    }
}

const BUF_SIZE: usize = 256;

#[inline]
fn last_newline(s: &[u8]) -> usize {
    let mut i = s.len() - 1;
    while i > 0 {
        if s[i] == b'\n' {
            return i + 1;
        }
        i -= 1;
    }
    s.len()
}
unsafe fn run() {
    let f = File::open("./measurements.txt").unwrap();
    let metadata = std::fs::metadata("./measurements.txt").unwrap();
    let mem_map = Arc::new(MmapOptions::new().map(&f).unwrap());
    let default_thread_counts = std::thread::available_parallelism().unwrap().get();

    let thread_counts = {
        if let Some((t, c)) = std::env::args().skip(1).take(2).next_tuple() {
            if t == "--num-threads" || t == "--threads" || t == "-t" {
                c.trim()
                    .parse::<usize>()
                    .expect("useage: -t <number_of_threads>")
            } else {
                default_thread_counts
            }
        } else {
            default_thread_counts
        }
    };
    println!("thread counts: {thread_counts}");

    let mut measurements: HashMap<Vec<u8>, Measurement> = HashMap::with_capacity(BUF_SIZE);

    let mut thread_workers: VecDeque<JoinHandle<HashMap<Vec<u8>, Measurement>>> =
        VecDeque::with_capacity(thread_counts);

    let file_len = metadata.len() as usize;

    let max_chunks_size = file_len / thread_counts;
    let mut offset = 0;

    let mut now = SystemTime::now();

    while offset < file_len {
        let end = {
            let end = offset + max_chunks_size;
            if end > file_len {
                file_len
            } else {
                end
            }
        };
        let last_new_line = offset + last_newline(&mem_map[offset..end]);

        let mem_map = mem_map.clone();
        thread_workers.push_back(std::thread::spawn(move || {
            let mut measurements: HashMap<Vec<u8>, Measurement> = HashMap::with_capacity(BUF_SIZE);
            if cfg!(unix) {
                mem_map
                    .advise_range(Advice::Sequential, offset, last_new_line - offset)
                    .unwrap_unchecked();
            }
            let buffers = &mem_map[offset..last_new_line];
            let len = buffers.len();
            let mut signed: i32 = 1;
            let mut start_dec = None;
            let mut key = Vec::with_capacity(1024);
            let mut num = 0;
            let mut i;
            let mut j = 0;

            loop {
                i = j;
                j += 1;
                let v = &buffers[i];
                if v == &b';' {
                    continue;
                }
                if v == &b'-' {
                    signed = -1;
                    continue;
                }
                if v == &b'.' {
                    start_dec = Some(i + 1);
                    continue;
                }

                if v.is_ascii_digit() {
                    num = num * 10 + (v - b'0') as i32;

                    continue;
                }

                if v == &b'\n' {
                    if let Some(sd) = start_dec {
                        let number_dec = i - sd;
                        if number_dec & 1 == 1 {
                            num *= 100;
                        }
                    } else {
                        num *= 1000;
                    }

                    num *= signed;
                    let data: &[u8] = &key;

                    if let Some(measurement) = measurements.get_mut(data) {
                        if measurement.min > num {
                            measurement.min = num;
                        } else if measurement.max < num {
                            measurement.max = num;
                        }
                        measurement.tot += num as i64;
                        measurement.count += 1;
                    } else {
                        measurements.insert(
                            data.into(),
                            Measurement {
                                min: num,
                                max: num,
                                tot: num as i64,
                                count: 1,
                            },
                        );
                    }
                    key.clear();
                    num = 0;

                    start_dec = None;
                    signed = 1;
                    if j >= len {
                        break;
                    }
                    continue;
                }
                key.push(*v);
            }

            measurements
        }));

        offset = last_new_line;
    }
    if cfg!(debug) {
        println!(
            "took {:.2} millis to spawn threads",
            now.elapsed().unwrap_unchecked().as_millis()
        );
    }
    now = SystemTime::now();

    while let Some(handle) = thread_workers.pop_front() {
        let now2 = SystemTime::now();
        let a = handle.join().unwrap_unchecked();
        for (k, v) in a {
            if let Some(m) = measurements.get_mut(&k) {
                m.tot += v.tot;
                m.count += v.count;
                if m.min > v.min {
                    m.min = v.min;
                }
                if m.max < v.max {
                    m.max = v.max;
                }
            } else {
                measurements.insert(k, v);
            }
        }
        if cfg!(debug) {
            println!(
                "took {:.2} millis to join threads",
                now2.elapsed().unwrap_unchecked().as_millis()
            );
        }
    }
    if cfg!(debug) {
        println!(
            "took {:.2} millis to merge threads",
            now.elapsed().unwrap_unchecked().as_millis()
        );
    }
    let mut res = String::from_str("{").unwrap_unchecked();
    for (i, (k, m)) in measurements
        .iter()
        .map(|e| (std::str::from_utf8(e.0).unwrap_unchecked(), e.1))
        .sorted_by(|l, r| l.0.cmp(r.0))
        .enumerate()
    {
        res += &format!(
            "{}={:.1}/{:.1}/{:.1}",
            k,
            m.min as f64 / 1000.,
            m.tot as f64 / 1000. / m.count as f64,
            m.max as f64 / 1000.
        );
        if i < measurements.len() - 1 {
            res.push_str(", ")
        }
    }
    res.push('}');
    println!("{res}");
    println!();
}
