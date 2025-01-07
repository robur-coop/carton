open Digestif

let ( $ ) f g x = f (g x)

let git_identify =
  let pp_kind ppf = function
    | `A -> Fmt.string ppf "commit"
    | `B -> Fmt.string ppf "tree"
    | `C -> Fmt.string ppf "blob"
    | `D -> Fmt.string ppf "tag"
  in
  let init kind (len : Carton.Size.t) =
    let hdr = Fmt.str "%a %d\000" pp_kind kind (len :> int) in
    let ctx = SHA1.empty in
    SHA1.feed_string ctx hdr
  in
  let feed bstr ctx = SHA1.feed_bigstring ctx bstr in
  let serialize = SHA1.(Carton.Uid.unsafe_of_string $ to_raw_string $ get) in
  { Carton.First_pass.init; feed; serialize }

let kind_to_string fmt kind =
  match (fmt, kind) with
  | `Git, `A -> "commit"
  | `Git, `B -> "tree"
  | `Git, `C -> "blob"
  | `Git, `D -> "tag"
  | `Carton, kind -> Fmt.to_to_string Carton.Kind.pp kind

(* Ugly! *)
let rec mkdir = function
  | "." -> ()
  | path ->
      if Sys.file_exists path = false then begin
        mkdir (Filename.dirname path);
        try Unix.mkdir path 0o755 with Unix.(Unix_error (EEXIST, _, _)) -> ()
      end
      else if Sys.is_directory path = false then
        Fmt.failwith "%s already exists as a file" path

type fmt =
  | Fmt :
      string
      * (string -> string -> string, Format.formatter, unit, string) format4
      -> fmt

let on queue fmt_kind (Fmt (_, fmt)) value (uid : Carton.Uid.t) =
  let kind = Carton.Value.kind value in
  let path =
    Fmt.str fmt (kind_to_string fmt_kind kind) (Ohex.encode (uid :> string))
  in
  let bstr = Carton.Value.bigstring value in
  let len = Carton.Value.length value in
  let bstr = Cachet.Bstr.of_bigstring (Bigarray.Array1.sub bstr 0 len) in
  let buf = Bytes.create 0x7ff in
  let rec go oc src_off max =
    if max > 0 then begin
      let len = Int.min max (Bytes.length buf) in
      Cachet.Bstr.blit_to_bytes bstr ~src_off buf ~dst_off:0 ~len;
      output_substring oc (Bytes.unsafe_to_string buf) 0 len;
      go oc (src_off + len) (max - len)
    end
  in
  mkdir (Filename.dirname path);
  Miou.Queue.enqueue queue (kind, path);
  let oc = open_out path in
  let finally () = close_out oc in
  Fun.protect ~finally @@ fun () -> go oc 0 len

let run quiet threads pagesize digest fmt_kind fmt_path (Fmt (_, fmt_output))
    pack =
  Miou_unix.run ~domains:threads @@ fun () ->
  let ref_length = Digestif.SHA1.digest_size in
  let identify = Carton.Identify git_identify in
  let queue = Miou.Queue.create () in
  let on_object ~cursor:_ uid value = on queue fmt_kind fmt_path uid value in
  let cfg =
    Carton_miou_unix.config ~threads ~pagesize ~ref_length ~on_object identify
  in
  let _matrix, _hash =
    match pack with
    | `Pack filename -> Carton_miou_unix.verify_from_pack ~cfg ~digest filename
    | `Idx (filename, _) ->
        Carton_miou_unix.verify_from_idx ~cfg ~digest filename
  in
  let entries = Miou.Queue.to_list queue in
  let show (kind, path) =
    let str = Fmt.str fmt_output (kind_to_string fmt_kind kind) path in
    print_endline str
  in
  if not quiet then Stdlib.List.iter show entries;
  Ok ()

open Cmdliner
open Args

let fmt_kind =
  let open Arg in
  let a_la_git =
    info [ "type-as-git" ] ~doc:"Print out the type of an object like Git."
  in
  let a_la_carton =
    info [ "type-as-carton" ]
      ~doc:
        "Print out the type of an object with $(b,A), $(b,B), $(b,C) & $(b,D)."
  in
  value & vflag `Carton [ (`Git, a_la_git); (`Carton, a_la_carton) ]

let fmt =
  let parser str =
    try
      let fmt = CamlinternalFormat.format_of_string_format str "%s%s" in
      Ok (Fmt (str, fmt))
    with _ -> error_msgf "Invalid format %S" str
  in
  let pp ppf (Fmt (str, _)) = Fmt.string ppf str in
  Arg.conv (parser, pp)

let fmt_path =
  let doc =
    "The format of the path to store objects from the given PACK file."
  in
  Arg.(required & pos 0 (some fmt) None & info [] ~doc ~docv:"FMT")

let fmt_output =
  let doc = "The format of the output for each entry in the given PACK file." in
  let default_fmt = Arg.conv_parser fmt "%s %s" in
  let default_fmt = Result.get_ok default_fmt in
  Arg.(value & opt fmt default_fmt & info [ "fmt-output" ] ~doc)

let pack =
  let doc =
    "The file used to access to the PACK file (it can be the PACK file \
     directly or the associated IDX file)."
  in
  Arg.(required & pos 1 (some pack) None & info [] ~doc ~docv:"FILE")

let term =
  let open Term in
  const run
  $ setup_logs
  $ threads
  $ pagesize
  $ setup_signature
  $ fmt_kind
  $ fmt_path
  $ fmt_output
  $ pack

let cmd : (unit, string) result Cmd.t =
  let doc = "A tool to $(i,explode) the given PACK file into files." in
  let man = [] in
  let info = Cmd.info "explode" ~doc ~man in
  Cmd.v info term
