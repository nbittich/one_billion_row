use crossbeam::channel::Sender;
use itertools::Itertools;
use std::collections::VecDeque;
use std::io::Read;
use std::str::FromStr;
use std::u16;
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
#[inline]
unsafe fn process_measurement(
    key: &[u8],
    num: &[u8],
    dec: Option<&[u8]>,
    signed: u16,
    measurements: &mut HashMap<Vec<u8>, Measurement>,
) {
    let to_num = |num: &[u8], signed: u16| {
        let mut integer = 0;
        for n in num {
            integer = integer * 10 + (n - b'0') as i32;
        }

        if signed == 1 {
            integer = -integer;
        }
        integer
    };
    let mut v = num.to_vec();

    if let Some(dec) = dec {
        v.extend_from_slice(dec);
        if dec.len() != 3 {
            v.extend_from_slice(&[b'0', b'0']);
        }
    } else {
        v.extend_from_slice(&[b'0', b'0', b'0']);
    }

    let n = to_num(&v, signed);

    if let Some(measurement) = measurements.get_mut(key) {
        if measurement.min > n {
            measurement.min = n;
        } else if measurement.max < n {
            measurement.max = n;
        }
        measurement.tot += n as i64;
        measurement.count += 1;
    } else {
        measurements.insert(
            key.into(),
            Measurement {
                min: n,
                max: n,
                tot: n as i64,
                count: 1,
            },
        );
    }
}
struct Worker {
    sender: Sender<Vec<u8>>,
    handle: JoinHandle<HashMap<Vec<u8>, Measurement>>,
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
impl Worker {
    fn new() -> Self {
        let (sender, receiver) = crossbeam::channel::bounded(1);
        Self {
            sender,
            handle: std::thread::spawn(move || {
                let mut measurements: HashMap<Vec<u8>, Measurement> =
                    HashMap::with_capacity(BUF_SIZE);
                loop {
                    match receiver.recv() {
                        Ok(buffers) => unsafe {
                            let mut start_key = 0;
                            let mut end_key = 0;
                            let mut start_num = 0;
                            let mut end_num = 0;
                            let mut signed = 0;
                            let mut start_dec = None;
                            let mut end_dec = None;

                            for i in 0..buffers.len() {
                                if buffers[i] == b';' {
                                    end_key = i;
                                    start_num = i + 1;
                                    continue;
                                }
                                if buffers[i] == b'-' {
                                    signed = 1;
                                    start_num = i + 1;
                                    continue;
                                }
                                if buffers[i] == b'.' {
                                    end_num = i;
                                    start_dec = Some(i + 1);
                                    continue;
                                }

                                if buffers[i] == b'\n' {
                                    if start_dec.is_some() {
                                        end_dec = Some(i);
                                    } else {
                                        end_num = i;
                                    }
                                    let dec = if let (Some(start_dec), Some(end_dec)) =
                                        (start_dec, end_dec)
                                    {
                                        Some(&buffers[start_dec..end_dec])
                                    } else {
                                        None
                                    };

                                    process_measurement(
                                        &buffers[start_key..end_key],
                                        &buffers[start_num..end_num],
                                        dec,
                                        signed,
                                        &mut measurements,
                                    );
                                    start_key = i + 1;

                                    start_dec = None;
                                    end_dec = None;
                                    signed = 0;
                                }
                            }
                        },
                        Err(_) => {
                            break;
                        }
                    }
                }

                measurements
            }),
        }
    }
}

const CHUNK_SIZE: usize = 64_000_000;

unsafe fn run() {
    let mut f = File::open("./measurements.txt").unwrap();
    let thread_counts = std::thread::available_parallelism().unwrap().get();
    let mut chunk = Vec::with_capacity(CHUNK_SIZE);

    let mut measurements: HashMap<Vec<u8>, Measurement> = HashMap::with_capacity(BUF_SIZE);

    let mut workers: VecDeque<Worker> = VecDeque::with_capacity(thread_counts);
    for _ in 0..thread_counts - 1 {
        workers.push_back(Worker::new());
    }

    loop {
        f.by_ref()
            .take((CHUNK_SIZE - chunk.len()) as u64)
            .read_to_end(&mut chunk)
            .unwrap_unchecked();

        if chunk.is_empty() {
            break;
        }
        let last_newline = last_newline(&chunk);
        let mut next_chunk = Vec::with_capacity(CHUNK_SIZE);
        next_chunk.extend_from_slice(&chunk[last_newline..]);
        let worker = workers.pop_front().unwrap_unchecked();
        worker.sender.send(chunk).unwrap_unchecked();
        workers.push_back(worker);
        chunk = next_chunk;
    }

    while let Some(Worker { handle, sender }) = workers.pop_front() {
        drop(sender);
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
