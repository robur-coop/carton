let error_msgf fmt = Fmt.kstr (fun msg -> Error (`Msg msg)) fmt
let ( % ) f g = fun x -> f (g x)

open Cmdliner

let output_options = "OUTPUT OPTIONS"
let () = Logs_threaded.enable ()

let verbosity =
  let env = Cmd.Env.info "CARTON_LOGS" in
  Logs_cli.level ~docs:output_options ~env ()

let renderer =
  let env = Cmd.Env.info "CARTON_FMT" in
  Fmt_cli.style_renderer ~docs:output_options ~env ()

let utf_8 =
  let doc = "Allow binaries to emit UTF-8 characters." in
  let env = Cmd.Env.info "CARTON_UTF_8" in
  Arg.(value & opt bool true & info [ "with-utf-8" ] ~doc ~env)

let reporter ppf =
  let report src level ~over k msgf =
    let k _ = over (); k () in
    let with_metadata header _tags k ppf fmt =
      Fmt.kpf k ppf
        ("%a[%a][%a]: " ^^ fmt ^^ "\n%!")
        Logs_fmt.pp_header (level, header)
        Fmt.(styled `Cyan int)
        (Stdlib.Domain.self () :> int)
        Fmt.(styled `Magenta string)
        (Logs.Src.name src)
    in
    msgf @@ fun ?header ?tags fmt -> with_metadata header tags k ppf fmt
  in
  { Logs.report }

let setup_logs utf_8 style_renderer level =
  Fmt_tty.setup_std_outputs ~utf_8 ?style_renderer ();
  let style_renderer =
    match style_renderer with
    | Some `Ansi_tty -> `Ansi
    | Some `None -> `None
    | None ->
        let dumb =
          match Sys.getenv_opt "TERM" with
          | Some "dumb" | Some "" | None -> true
          | _ -> false
        in
        let isatty =
          try Unix.(isatty (descr_of_out_channel Stdlib.stdout))
          with Unix.Unix_error _ -> false
        in
        if (not dumb) && isatty then `Ansi else `None
  in
  Hxd.Fmt.set_style_renderer Fmt.stdout style_renderer;
  let reporter = reporter Fmt.stderr in
  Logs.set_reporter reporter; Logs.set_level level; Option.is_none level

let setup_logs = Term.(const setup_logs $ utf_8 $ renderer $ verbosity)
let docs_hexdump = "HEX OUTPUT"

let colorscheme =
  let x = Array.make 256 `None in
  for i = 0 to 31 do
    x.(i) <- `Style (`Fg, `bit24 (0xaf, 0xd7, 0xff))
  done;
  for i = 48 to 57 do
    x.(i) <- `Style (`Fg, `bit24 (0xaf, 0xdf, 0x77))
  done;
  for i = 65 to 90 do
    x.(i) <- `Style (`Fg, `bit24 (0xff, 0xaf, 0x5f))
  done;
  for i = 97 to 122 do
    x.(i) <- `Style (`Fg, `bit24 (0xff, 0xaf, 0xd7))
  done;
  Hxd.colorscheme_of_array x

let cols =
  let doc = "Format $(i,COLS) octets per line. Default 16. Max 256." in
  let parser str =
    match int_of_string str with
    | n when n < 1 || n > 256 ->
        error_msgf "Invalid COLS value (must <= 256 && > 0): %d" n
    | n -> Ok n
    | exception _ -> error_msgf "Invalid COLS value: %S" str
  in
  let open Arg in
  let cols = conv (parser, Fmt.int) in
  value
  & opt (some cols) None
  & info [ "c"; "cols" ] ~doc ~docv:"COLS" ~docs:docs_hexdump

let groupsize =
  let doc =
    "Separate the output of every $(i,bytes) bytes (two hex characters) by a \
     whitespace. Specify -g 0 to supress grouping. $(i,bytes) defaults to 2."
  in
  let open Arg in
  value
  & opt (some int) None
  & info [ "g"; "groupsize" ] ~doc ~docv:"BYTES" ~docs:docs_hexdump

let len =
  let doc = "Stop after writing $(i,LEN) octets." in
  let open Arg in
  value
  & opt (some int) None
  & info [ "l"; "len" ] ~doc ~docv:"LEN" ~docs:docs_hexdump

let uppercase =
  let doc = "Use upper case hex letters. Default is lower case." in
  let open Arg in
  value & flag & info [ "u" ] ~doc ~docs:docs_hexdump

let setup_hxd cols groupsize len uppercase =
  Hxd.xxd ?cols ?groupsize ?long:len ~uppercase colorscheme

let setup_hxd = Term.(const setup_hxd $ cols $ groupsize $ len $ uppercase)

type signature = [ `SHA1 | `SHA256 | `SHA512 | `SHA3_256 | `SHA3_512 | `BLAKE2B ]

let setup_signature hash =
  let module Hash = (val Digestif.module_of_hash' (hash :> Digestif.hash')) in
  let feed_bigstring bstr ctx = Hash.feed_bigstring ctx bstr in
  let feed_bytes buf ~off ~len ctx = Hash.feed_bytes ctx ~off ~len buf in
  let hash =
    {
      Carton.First_pass.feed_bytes
    ; feed_bigstring
    ; serialize= Hash.to_raw_string % Hash.get
    ; length= Hash.digest_size
    }
  in
  Carton.First_pass.Digest (hash, Hash.empty)

let signature =
  let parser str =
    match String.lowercase_ascii str with
    | "sha1" -> Ok `SHA1
    | "sha256" -> Ok `SHA256
    | "sha512" -> Ok `SHA512
    | "sha3-256" -> Ok `SHA3_256
    | "sha3-512" -> Ok `SHA3_512
    | "blake2b" -> Ok `BLAKE2B
    | _ -> error_msgf "Invalid hash algorithm: %S" str
  in
  let pp ppf = function
    | `SHA1 -> Fmt.string ppf "sha1"
    | `SHA256 -> Fmt.string ppf "sha256"
    | `SHA512 -> Fmt.string ppf "sha512"
    | `SHA3_256 -> Fmt.string ppf "sha3-256"
    | `SHA3_512 -> Fmt.string ppf "sha3-512"
    | `BLAKE2B -> Fmt.string ppf "blake2b"
  in
  Arg.conv (parser, pp)

let signature =
  let doc = "The hash algorithm used to verify the PACK integrity." in
  Arg.(value & opt signature `SHA1 & info [ "signature" ] ~doc ~docv:"HASH")

let setup_signature = Term.(const setup_signature $ signature)
let setup_progress max_width = Progress.Config.v ~max_width ()

let width =
  let doc = "Width of the terminal." in
  let default = Terminal.Size.get_columns () in
  let open Arg in
  value & opt (some int) default & info [ "width" ] ~doc ~docv:"WIDTH"

let setup_progress = Term.(const setup_progress $ width)

let pack =
  let parser str =
    match Fpath.of_string str with
    | Ok value when Sys.file_exists str && Sys.is_directory str = false -> begin
        match Fpath.get_ext value with
        | ".pack" -> Ok (`Pack value)
        | ".idx" ->
            let pack = Fpath.set_ext ".pack" value in
            if
              Sys.file_exists (Fpath.to_string pack)
              && Sys.is_directory (Fpath.to_string pack) = false
            then Ok (`Idx (value, pack))
            else
              error_msgf "The associated PACK file to %a does not exist"
                Fpath.pp value
        | _ -> error_msgf "Unexpected file %a" Fpath.pp value
      end
    | Ok value -> error_msgf "%a does not exist" Fpath.pp value
    | Error _ as err -> err
  in
  let pp ppf = function `Pack value | `Idx (value, _) -> Fpath.pp ppf value in
  Arg.conv (parser, pp) ~docv:"FILE"

external getpagesize : unit -> int = "carton_miou_unix_getpagesize" [@@noalloc]

let default_domains = Int.min 4 (Stdlib.Domain.recommended_domain_count ())

let threads =
  let doc = "The number of threads to use to verify objects." in
  Arg.(value & opt int default_domains & info [ "threads" ] ~doc ~docv:"NUMBER")

let pagesize =
  let doc =
    "The memory page size to use to verify the given PACK file (must be a \
     power of two)."
  in
  let open Arg in
  value & opt int (getpagesize ()) & info [ "pagesize" ] ~doc ~docv:"BYTES"
