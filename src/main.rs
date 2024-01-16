use memmap2::{Advice, Mmap, MmapOptions};
use std::collections::{BTreeSet, VecDeque};
use std::str::FromStr;
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
#[inline]
unsafe fn run() {
    let f = File::open("./measurements.txt").unwrap();
    let metadata = std::fs::metadata("./measurements.txt").unwrap();
    let mem_map = MmapOptions::new().map(&f).unwrap();
    let mem_map: &'static Mmap = std::mem::transmute(&mem_map);
    let default_thread_counts = std::thread::available_parallelism().unwrap().get();

    let thread_counts = {
        let thread_arg = std::env::args().skip(1).take(2).collect::<Vec<_>>();
        if let [t, c, ..] = thread_arg.as_slice() {
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

    let mut max_chunks_size = file_len / thread_counts;
    max_chunks_size -= max_chunks_size % 4096;
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

        thread_workers.push_back(std::thread::spawn(move || {
            let mut measurements: HashMap<Vec<u8>, Measurement> = HashMap::with_capacity(BUF_SIZE);
            if cfg!(unix) {
                mem_map
                    .advise_range(Advice::Sequential, offset, last_new_line - offset)
                    .unwrap_unchecked();
            }
            let mut signed = 1;
            let mut key = [0; 100];

            let mut num = 0;
            let mut next_k = 0;
            let mut end_k = 0;

            for v in &mem_map[offset..last_new_line] {
                if end_k == 0 {
                    if v == &b';' {
                        end_k = next_k;
                    } else {
                        key[next_k] = *v;
                        next_k += 1;
                    }
                } else if v == &b'\n' {
                    num *= signed;
                    let data: &[u8] = &key[0..end_k];

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

                    num = 0;
                    next_k = 0;
                    end_k = 0;
                    signed = 1;
                } else {
                    if v == &b'-' {
                        signed *= -1;
                    } else if v == &b'.' {
                    } else {
                        num = ((num << 3) + (num << 1)) + (v - b'0') as i32;
                    }
                }
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
        if handle.is_finished() {
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
        } else {
            thread_workers.push_back(handle);
        }
    }
    if cfg!(debug) {
        println!(
            "took {:.2} millis to merge threads",
            now.elapsed().unwrap_unchecked().as_millis()
        );
    }
    let mut res = String::from_str("{").unwrap_unchecked();

    let ml = measurements.len();
    let keys = measurements.keys().collect::<BTreeSet<_>>();
    for (i, (k, m)) in keys
        .into_iter()
        .map(|k| measurements.get_key_value(k).unwrap_unchecked())
        .map(|e| (std::str::from_utf8(&e.0[..]).unwrap_unchecked(), e.1))
        .enumerate()
    {
        res += &format!(
            "{}={:.1}/{:.1}/{:.1}",
            k,
            (m.min * 100) as f64 / 1000.,
            (m.tot * 100) as f64 / 1000. / m.count as f64,
            (m.max * 100) as f64 / 1000.
        );
        if i < ml - 1 {
            res.push_str(", ")
        }
    }
    res.push('}');
    println!("{res}");
}
