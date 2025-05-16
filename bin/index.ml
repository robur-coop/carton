open Digestif

let ( $ ) f g x = f (g x)
let error_msgf fmt = Fmt.kstr (fun msg -> Error (`Msg msg)) fmt

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

let entries_of_pack cfg digest pack =
  let matrix, hash =
    match pack with
    | `Pack filename -> Carton_miou_unix.verify_from_pack ~cfg ~digest filename
    | `Idx (filename, _) ->
        Carton_miou_unix.verify_from_idx ~cfg ~digest filename
  in
  let fn _idx = function
    | Carton.Unresolved_base _ | Carton.Unresolved_node _ ->
        Logs.err (fun m -> m "object %d unresolved" _idx);
        assert false
    | Resolved_base { cursor; uid; crc; _ } ->
        let uid = Classeur.unsafe_uid_of_string (uid :> string) in
        Classeur.Encoder.{ uid; crc; offset= Int64.of_int cursor }
    | Resolved_node { cursor; uid; crc; _ } ->
        let uid = Classeur.unsafe_uid_of_string (uid :> string) in
        Classeur.Encoder.{ uid; crc; offset= Int64.of_int cursor }
  in
  (Array.mapi fn matrix, hash)

let signature_of_filename filename =
  let buf = Bytes.create 0x7ff in
  let ic = open_in (Fpath.to_string filename) in
  let finally () = close_in ic in
  Fun.protect ~finally @@ fun () ->
  let rec go ctx =
    match input ic buf 0 (Bytes.length buf) with
    | 0 -> BLAKE2B.get ctx
    | len -> go (BLAKE2B.feed_bytes ctx ~off:0 ~len buf)
    | exception End_of_file -> BLAKE2B.get ctx
  in
  go BLAKE2B.empty

let verify cfg digest pack output =
  let entries, pack = entries_of_pack cfg digest pack in
  let encoder =
    Classeur.Encoder.encoder `Manual ~digest ~pack ~ref_length:cfg.ref_length
      entries
  in
  let out = Bytes.create 0x7ff in
  let rec go ctx (`Await as await) =
    match Classeur.Encoder.encode encoder await with
    | `Ok ->
        let len = Bytes.length out - Classeur.Encoder.dst_rem encoder in
        let ctx = BLAKE2B.feed_bytes ctx ~off:0 ~len out in
        BLAKE2B.get ctx
    | `Partial ->
        let len = Bytes.length out - Classeur.Encoder.dst_rem encoder in
        let ctx = BLAKE2B.feed_bytes ctx ~off:0 ~len out in
        Classeur.Encoder.dst encoder out 0 (Bytes.length out);
        go ctx await
  in
  Classeur.Encoder.dst encoder out 0 (Bytes.length out);
  let signature = go BLAKE2B.empty `Await in
  if BLAKE2B.equal signature (signature_of_filename output) then Ok ()
  else error_msgf "Invalid IDX file"

let generate cfg digest pack output =
  let entries, pack = entries_of_pack cfg digest pack in
  let encoder =
    Classeur.Encoder.encoder `Manual ~digest ~pack ~ref_length:cfg.ref_length
      entries
  in
  let oc = open_out (Fpath.to_string output) in
  let finally () = close_out oc in
  Fun.protect ~finally @@ fun () ->
  let out = Bytes.create 0x7ff in
  let rec go (`Await as await) =
    match Classeur.Encoder.encode encoder await with
    | `Ok ->
        let len = Bytes.length out - Classeur.Encoder.dst_rem encoder in
        output_substring oc (Bytes.unsafe_to_string out) 0 len
    | `Partial ->
        let len = Bytes.length out - Classeur.Encoder.dst_rem encoder in
        output_substring oc (Bytes.unsafe_to_string out) 0 len;
        Classeur.Encoder.dst encoder out 0 (Bytes.length out);
        go await
  in
  Classeur.Encoder.dst encoder out 0 (Bytes.length out);
  go `Await;
  Ok ()

let run _quiet threads pagesize digest pack output =
  Miou_unix.run ~domains:threads @@ fun () ->
  let output =
    match (pack, output) with
    | `Pack pack, None -> Fpath.set_ext ".idx" pack
    | `Idx (_, _), Some idx -> idx
    | `Idx (idx, _), None -> idx
    | `Pack _, Some idx -> idx
  in
  let action =
    if Sys.file_exists (Fpath.to_string output) then `Verify else `Generate
  in
  let ref_length = SHA1.digest_size in
  let identify = Carton.Identify git_identify in
  let cfg = Carton_miou_unix.config ~threads ~pagesize ~ref_length identify in
  let result =
    match action with
    | `Verify -> verify cfg digest pack output
    | `Generate -> generate cfg digest pack output
  in
  Result.map_error (fun (`Msg msg) -> msg) result

open Cmdliner
open Args

let pack =
  let doc =
    "The file used to access to the PACK file (it can be the PACK file \
     directly or the associated IDX file)."
  in
  Arg.(required & pos 0 (some pack) None & info [] ~doc ~docv:"FILE")

let output =
  let doc = "The IDX file to verify or to generate." in
  let filename = Arg.conv Fpath.(of_string, pp) in
  Arg.(value & opt (some filename) None & info [ "o"; "output" ] ~doc)

let term =
  let open Term in
  const run $ setup_logs $ threads $ pagesize $ setup_signature $ pack $ output

let cmd =
  let doc = "A tool to generate or verify an IDX file." in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "A PACK file is a succession of objects with two-level compression. To \
         access these objects, the user needs to know their $(b,offsets) (see \
         the $(b,get) command). However, objects can be identified by a unique \
         reference which, in the case of Git, corresponds to a SHA1 hash. The \
         IDX file is used to associate these references with the offsets in a \
         PACK file. In this way, the user can indirectly access these objects \
         via their unique reference."
    ; `P
        "Calculating and generating an IDX file is relatively quick. This \
         allows “random” access to objects according to their unique \
         references."
    ; `P
        "$(tname) is used to generate or verify an IDX file. If the user calls \
         $(b,carton index) with a PACK file, the tool generates the associated \
         IDX file with the same name."
    ; `P
        "$(tname) can also verify the integrity of an IDX file according to \
         its associated PACK file."
    ; `P "Here are a few examples, with comments explaining what $(tname) does:"
    ; `Pre
        "\\$ carton index pack.pack # it generates pack.idx\n\
         \\$ carton index pack.idx # it verifies the integrity of pack.idx \
         according to pack.pack\n\
         \\$ carton index pack.idx -o new.idx # it generates new.idx according \
         to pack.pack"
    ; `P
        "Generating an IDX file can take advantage of multiple cores. Indeed, \
         the underlying calculations to characterize the unique object \
         references are highly parallelizable. You can determine the number of \
         cores you require using the $(b,--threads) option."
    ]
  in
  let info = Cmd.info "index" ~doc ~man in
  Cmd.v info term
