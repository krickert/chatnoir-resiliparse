// Preflight check: is a .warc.gz splittable for `config.parallelism`?
//
// Scans a prefix of the file for gzip member boundaries, validating each
// magic-byte candidate the same way the server does (decompress a prefix
// and require it to start with `WARC/`). Reports member density and a
// verdict.
use std::io::Read;

const GZIP_MAGIC: [u8; 3] = [0x1f, 0x8b, 0x08];
const SCAN_LIMIT: usize = 32 << 20;
const VALIDATE_WINDOW: usize = 4096;

fn is_warc_gzip_member(buf: &[u8]) -> bool {
    let head = &buf[..buf.len().min(VALIDATE_WINDOW)];
    let mut reader = fastwarc::stream_io::gzip::GzipReader::new(std::io::Cursor::new(head.to_vec()));
    let mut magic = [0u8; 5];
    reader.read_exact(&mut magic).is_ok() && &magic == b"WARC/"
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        println!("Usage: {} WARCFILE.warc.gz", args[0]);
        println!("Reports whether the archive is member-per-record gzip (splittable");
        println!("by config.parallelism) by scanning the first 32 MiB.");
        return;
    }
    let mut file = std::fs::File::open(&args[1]).expect("File error");
    let mut buf = Vec::with_capacity(SCAN_LIMIT);
    Read::take(Read::by_ref(&mut file), SCAN_LIMIT as u64)
        .read_to_end(&mut buf)
        .expect("read error");
    let scanned_mib = buf.len() as f64 / 1024.0 / 1024.0;

    if buf.len() < 3 || buf[..3] != GZIP_MAGIC {
        println!("{}: not gzip; parallelism falls back to a sequential parse", args[1]);
        return;
    }

    let mut members = 0usize;
    let mut candidates = 0usize;
    let mut pos = 0usize;
    while pos + GZIP_MAGIC.len() <= buf.len() {
        let Some(rel) = buf[pos..].windows(GZIP_MAGIC.len()).position(|w| w == GZIP_MAGIC) else {
            break;
        };
        let candidate = pos + rel;
        candidates += 1;
        if is_warc_gzip_member(&buf[candidate..]) {
            members += 1;
            // Skip ahead a little; members are never this dense.
            pos = candidate + 128;
        } else {
            pos = candidate + 1;
        }
    }

    if members >= 2 {
        println!(
            "{}: member-per-record gzip; {} WARC members in the first {:.1} MiB \
             (avg {:.1} KiB compressed, {} magic false positives rejected)",
            args[1],
            members,
            scanned_mib,
            buf.len() as f64 / members as f64 / 1024.0,
            candidates - members,
        );
        println!("parallelism will split this archive");
    } else {
        println!("{}: gzip, but only {} WARC member boundary in the first {:.1} MiB", args[1], members, scanned_mib);
        println!("parallelism will fall back to a sequential parse");
    }
}
