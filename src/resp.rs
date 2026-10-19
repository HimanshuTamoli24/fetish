use crate::engine::Db;

/// Maximum allowed bulk string length (16 MB) to prevent denial of service.
pub const MAX_BULK_LEN: usize = 16 * 1024 * 1024;
/// Maximum allowed elements in an array.
pub const MAX_ARRAY_LEN: usize = 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum ParseResult<T> {
    Complete(T, usize), // (item, bytes_consumed)
    Incomplete,
    Error(String),
}

/// Parses a single RESP command frame from a byte buffer.
/// Returns Ok(Some((command_parts, consumed_bytes))), Ok(None) if incomplete, or Err(msg) if malformed.
pub fn parse_resp_command(buf: &[u8]) -> Result<Option<(Vec<String>, usize)>, String> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'*' => parse_resp_array(buf),
        // Also support inline commands (e.g. "GET key\r\n" or "SET a b\r\n") for raw client testing
        _ => parse_inline_command(buf),
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

fn parse_resp_array(buf: &[u8]) -> Result<Option<(Vec<String>, usize)>, String> {
    let crlf = match find_crlf(buf) {
        Some(pos) => pos,
        None => return Ok(None),
    };

    let len_str = std::str::from_utf8(&buf[1..crlf])
        .map_err(|_| "Invalid UTF-8 in array length".to_string())?;
    let count: i64 = len_str
        .parse()
        .map_err(|_| "Invalid array length integer".to_string())?;

    if count < 0 {
        return Err("Null array not allowed as command".to_string());
    }
    let count = count as usize;
    if count > MAX_ARRAY_LEN {
        return Err(format!("Array length exceeds max limit of {}", MAX_ARRAY_LEN));
    }

    let mut cursor = crlf + 2;
    let mut elements = Vec::with_capacity(count);

    for _ in 0..count {
        if cursor >= buf.len() {
            return Ok(None);
        }

        if buf[cursor] != b'$' {
            return Err("Expected bulk string '$' in command array".to_string());
        }

        let bulk_crlf = match find_crlf(&buf[cursor..]) {
            Some(pos) => cursor + pos,
            None => return Ok(None),
        };

        let bulk_len_str = std::str::from_utf8(&buf[cursor + 1..bulk_crlf])
            .map_err(|_| "Invalid UTF-8 in bulk string length".to_string())?;
        let bulk_len: i64 = bulk_len_str
            .parse()
            .map_err(|_| "Invalid bulk length integer".to_string())?;

        if bulk_len < 0 {
            return Err("Null bulk string in command not supported".to_string());
        }
        let bulk_len = bulk_len as usize;
        if bulk_len > MAX_BULK_LEN {
            return Err(format!("Bulk string exceeds max size of {}", MAX_BULK_LEN));
        }

        let data_start = bulk_crlf + 2;
        let data_end = data_start + bulk_len;

        // Need data + \r\n
        if buf.len() < data_end + 2 {
            return Ok(None);
        }

        if buf[data_end] != b'\r' || buf[data_end + 1] != b'\n' {
            return Err("Expected CRLF after bulk string data".to_string());
        }

        let s = std::str::from_utf8(&buf[data_start..data_end])
            .map_err(|_| "Bulk string data is not valid UTF-8".to_string())?;
        elements.push(s.to_string());

        cursor = data_end + 2;
    }

    Ok(Some((elements, cursor)))
}

fn parse_inline_command(buf: &[u8]) -> Result<Option<(Vec<String>, usize)>, String> {
    let crlf = match find_crlf(buf) {
        Some(pos) => pos,
        None => return Ok(None),
    };

    let line = std::str::from_utf8(&buf[..crlf])
        .map_err(|_| "Invalid UTF-8 in inline command".to_string())?;
    let elements: Vec<String> = line
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();

    if elements.is_empty() {
        return Ok(Some((Vec::new(), crlf + 2)));
    }

    Ok(Some((elements, crlf + 2)))
}

/// Executes a parsed RESP command and formats the RESP2 response.
pub fn execute_command(db: &Db, args: &[String]) -> String {
    if args.is_empty() {
        return "-ERR empty command\r\n".to_string();
    }

    let cmd = args[0].to_ascii_uppercase();

    match cmd.as_str() {
        "GET" => {
            if args.len() != 2 {
                return "-ERR wrong number of arguments for 'get' command\r\n".to_string();
            }
            match db.get(&args[1]) {
                Some(val) => format!("${}\r\n{}\r\n", val.len(), val),
                None => "$-1\r\n".to_string(),
            }
        }
        "SET" => {
            if args.len() == 3 {
                db.s(&args[1], &args[2]);
                "+OK\r\n".to_string()
            } else if args.len() == 5 && args[3].eq_ignore_ascii_case("EX") {
                match args[4].parse::<u64>() {
                    Ok(secs) if secs > 0 => {
                        match db.set(&args[1], &args[2], Some(secs)) {
                            Ok(_) => "+OK\r\n".to_string(),
                            Err(e) => format!("-ERR {}\r\n", e),
                        }
                    }
                    _ => "-ERR value is not an integer or out of range\r\n".to_string(),
                }
            } else {
                "-ERR syntax error\r\n".to_string()
            }
        }
        "DEL" => {
            if args.len() < 2 {
                return "-ERR wrong number of arguments for 'del' command\r\n".to_string();
            }
            let keys: Vec<&str> = args[1..].iter().map(|s| s.as_str()).collect();
            let count = db.del(&keys);
            format!(":{}\r\n", count)
        }
        "EXPIRE" => {
            if args.len() != 3 {
                return "-ERR wrong number of arguments for 'expire' command\r\n".to_string();
            }
            match args[2].parse::<i64>() {
                Ok(seconds) => {
                    let result = db.expire(&args[1], seconds);
                    format!(":{}\r\n", result)
                }
                Err(_) => "-ERR value is not an integer or out of range\r\n".to_string(),
            }
        }
        "TTL" => {
            if args.len() != 2 {
                return "-ERR wrong number of arguments for 'ttl' command\r\n".to_string();
            }
            let ttl = db.ttl(&args[1]);
            format!(":{}\r\n", ttl)
        }
        _ => format!("-ERR unknown command '{}'\r\n", args[0]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_resp_array() {
        let input = b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$8\r\nHimanshu\r\n";
        let res = parse_resp_command(input).unwrap();
        assert!(res.is_some());
        let (parts, consumed) = res.unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(parts, vec!["SET", "name", "Himanshu"]);
    }

    #[test]
    fn test_partial_read() {
        let input = b"*3\r\n$3\r\nSET\r\n$4\r\nna";
        let res = parse_resp_command(input).unwrap();
        assert_eq!(res, None);
    }

    #[test]
    fn test_multiple_commands_in_buffer() {
        let mut input = Vec::new();
        input.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
        input.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\nb\r\n");

        let (cmd1, consumed1) = parse_resp_command(&input).unwrap().unwrap();
        assert_eq!(cmd1, vec!["GET", "a"]);

        let (cmd2, consumed2) = parse_resp_command(&input[consumed1..]).unwrap().unwrap();
        assert_eq!(cmd2, vec!["GET", "b"]);
        assert_eq!(consumed1 + consumed2, input.len());
    }

    #[test]
    fn test_execute_get_set_del() {
        let db = Db::new();
        // SET
        let resp = execute_command(&db, &["SET".into(), "user".into(), "Alice".into()]);
        assert_eq!(resp, "+OK\r\n");

        // GET
        let resp = execute_command(&db, &["get".into(), "user".into()]);
        assert_eq!(resp, "$5\r\nAlice\r\n");

        // DEL
        let resp = execute_command(&db, &["del".into(), "user".into()]);
        assert_eq!(resp, ":1\r\n");

        // GET missing
        let resp = execute_command(&db, &["get".into(), "user".into()]);
        assert_eq!(resp, "$-1\r\n");
    }

    #[test]
    fn test_execute_set_ex_and_ttl() {
        let db = Db::new();
        let resp = execute_command(
            &db,
            &["SET".into(), "temp".into(), "val".into(), "EX".into(), "10".into()],
        );
        assert_eq!(resp, "+OK\r\n");

        let resp = execute_command(&db, &["TTL".into(), "temp".into()]);
        assert!(resp == ":10\r\n" || resp == ":9\r\n");
    }

    #[test]
    fn test_invalid_command() {
        let db = Db::new();
        let resp = execute_command(&db, &["FOOBAR".into()]);
        assert!(resp.starts_with("-ERR unknown command"));
    }

    #[test]
    fn test_invalid_argument_counts() {
        let db = Db::new();
        // GET with 0 args
        assert_eq!(execute_command(&db, &["GET".into()]), "-ERR wrong number of arguments for 'get' command\r\n");
        // DEL with 0 keys
        assert_eq!(execute_command(&db, &["DEL".into()]), "-ERR wrong number of arguments for 'del' command\r\n");
        // EXPIRE with missing seconds
        assert_eq!(execute_command(&db, &["EXPIRE".into(), "key".into()]), "-ERR wrong number of arguments for 'expire' command\r\n");
        // TTL with extra args
        assert_eq!(execute_command(&db, &["TTL".into(), "key".into(), "extra".into()]), "-ERR wrong number of arguments for 'ttl' command\r\n");
    }

    #[test]
    fn test_oversized_limits() {
        // Bulk length > MAX_BULK_LEN
        let input = format!("*1\r\n${}\r\n...\r\n", MAX_BULK_LEN + 1);
        assert!(parse_resp_command(input.as_bytes()).is_err());

        // Array length > MAX_ARRAY_LEN
        let input = format!("*{}\r\n", MAX_ARRAY_LEN + 1);
        assert!(parse_resp_command(input.as_bytes()).is_err());
    }
}
