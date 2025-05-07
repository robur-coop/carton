open Digestif

let ( $ ) f g = fun x -> f (g x)
let error_msgf fmt = Fmt.kstr (fun msg -> Error (`Msg msg)) fmt
let pp_ofs_delta ppf abs_offset = Fmt.pf ppf "Δ(%08x)" abs_offset
let pp_ref_delta ppf ptr = Fmt.pf ppf "Δ(%a)" Carton.Uid.pp ptr

let pp_kind ?uid ~offset ppf = function
  | Carton.First_pass.Base `A ->
      Fmt.pf ppf "%a %a"
        Fmt.(styled `Blue string)
        "a"
        Fmt.(styled `Yellow SHA1.pp)
        (Option.get uid)
  | Base `B ->
      Fmt.pf ppf "%a %a"
        Fmt.(styled `Cyan string)
        "b"
        Fmt.(styled `Yellow SHA1.pp)
        (Option.get uid)
  | Base `C ->
      Fmt.pf ppf "%a %a"
        Fmt.(styled `Green string)
        "c"
        Fmt.(styled `Yellow SHA1.pp)
        (Option.get uid)
  | Base `D ->
      Fmt.pf ppf "%a %a"
        Fmt.(styled `Magenta string)
        "d"
        Fmt.(styled `Yellow SHA1.pp)
        (Option.get uid)
  | Ofs { sub; _ } ->
      Fmt.pf ppf "%a" Fmt.(styled `Red pp_ofs_delta) (offset - sub)
  | Ref { ptr; _ } -> Fmt.pf ppf "%a" Fmt.(styled `Yellow pp_ref_delta) ptr

let pp_bytes ppf = function
  | (0 | 1) as value -> Fmt.pf ppf "%d byte" value
  | value -> Fmt.pf ppf "%d bytes" value

let seq_of_filename filename =
  let ic = open_in filename in
  let buf = Bytes.create 0x7ff in
  let dispenser () =
    match input ic buf 0 (Bytes.length buf) with
    | 0 -> close_in ic; None
    | len -> Some (Bytes.sub_string buf 0 len)
    | exception End_of_file -> close_in ic; None
  in
  Seq.of_dispenser dispenser

let seq_of_stdin () =
  let buf = Bytes.create 0x7ff in
  let dispenser () =
    match input stdin buf 0 (Bytes.length buf) with
    | 0 -> None
    | len -> Some (Bytes.sub_string buf 0 len)
    | exception End_of_file -> None
  in
  Seq.of_dispenser dispenser

let empty (kind, size) =
  let pp_kind ppf = function
    | `A -> Fmt.string ppf "commit"
    | `B -> Fmt.string ppf "tree"
    | `C -> Fmt.string ppf "blob"
    | `D -> Fmt.string ppf "tag"
  in
  let hdr = Fmt.str "%a %d\000" pp_kind kind size in
  let ctx = SHA1.empty in
  SHA1.feed_string ctx hdr

let run _ digest (_filename, seq) =
  let output = De.bigstring_create 0x7ff in
  let allocate bits = De.make_window ~bits in
  let ref_length = SHA1.digest_size in
  let seq =
    Carton.First_pass.of_seq ~output ~allocate ~ref_length ~digest seq
  in
  let rec go ctx seq =
    match Seq.uncons seq with
    | Some (`Number _, seq) -> go ctx seq
    | Some (`Inflate (None, _), seq) -> go ctx seq
    | Some (`Inflate (Some (kind, size), str), seq) -> begin
        match ctx with
        | None ->
            let ctx = empty (kind, size) in
            let ctx = SHA1.feed_string ctx str in
            go (Some ctx) seq
        | Some ctx ->
            let ctx = SHA1.feed_string ctx str in
            go (Some ctx) seq
      end
    | Some (`Entry entry, seq) ->
        let offset = entry.Carton.First_pass.offset in
        let kind = entry.Carton.First_pass.kind in
        let size = (entry.Carton.First_pass.size :> int) in
        let consumed = entry.Carton.First_pass.consumed in
        let crc = Optint.to_int32 entry.Carton.First_pass.crc in
        let uid = Option.map SHA1.get ctx in
        Fmt.pr "%010x: %a\n%!" offset (pp_kind ?uid ~offset) kind;
        Fmt.pr "      size: %a\n%!" pp_bytes (size :> int);
        Fmt.pr "  consumed: %a\n%!" pp_bytes consumed;
        Fmt.pr "       crc: %08lx\n%!" crc;
        go None seq
    | Some (`Hash hash, seq) ->
        Fmt.pr "%s\n%!" (Ohex.encode hash);
        go None seq
    | None -> Ok ()
  in
  go None seq

open Cmdliner
open Args

let input =
  let doc = "The PACK file to analyze." in
  let parser str =
    match str with
    | "-" -> Ok ("-", seq_of_stdin ())
    | filename ->
        if Sys.file_exists filename && not (Sys.is_directory filename) then
          Ok (filename, seq_of_filename filename)
        else error_msgf "%s does not exist" filename
  in
  let pp ppf (str, _) = Fmt.string ppf str in
  let input = Arg.conv (parser, pp) in
  Arg.(required & pos 0 (some input) None & info [] ~doc ~docv:"FILE")

let term =
  let open Term in
  const run $ setup_logs $ setup_signature $ input

let cmd : (unit, string) result Cmd.t =
  let doc = "A tool to list objects into the given PACK file." in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "$(tname) lists the objects in a PACK file. It does not extract them, \
         but collects useful information about the objects, such as their \
         position in the PACK file and their size in the PACK file (which \
         $(b,does not) correspond to the actual size of the objects)."
    ]
  in
  let info = Cmd.info "list" ~doc ~man in
  Cmd.v info term
