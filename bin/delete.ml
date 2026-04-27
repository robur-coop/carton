let ( $ ) f g x = f (g x)
let error_msgf fmt = Fmt.kstr (fun msg -> Error (`Msg msg)) fmt

let git_identify =
  let open Digestif in
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

let run _quiet threads pagesize digest src uids =
  Miou_unix.run ~domains:threads @@ fun () ->
  let ref_length = Digestif.SHA1.digest_size in
  let identify = Carton.Identify git_identify in
  let cfg = Carton_miou_unix.config ~threads ~pagesize ~ref_length identify in
  Carton_miou_unix.delete_in_place ~cfg ~digest ~pack:src uids;
  Ok ()

open Cmdliner
open Carton_cli

let src =
  let doc = "The source PACK file." in
  let parser str =
    match Fpath.of_string str with
    | Ok value when Sys.file_exists str && Sys.is_directory str = false ->
        begin match Fpath.get_ext value with
        | ".pack" -> Ok value
        | _ -> error_msgf "Unexpected file %a (must be .pack)" Fpath.pp value
        end
    | Ok value -> error_msgf "%a does not exist" Fpath.pp value
    | Error _ as err -> err
  in
  let existing_pack = Arg.conv (parser, Fpath.pp) in
  let open Arg in
  required
  & opt (some existing_pack) None
  & info [ "p"; "pack" ] ~doc ~docv:"FILE"

let uids =
  let doc = "The unique identifiers of the objects to remove." in
  let parser str =
    match Ohex.decode ~skip_whitespace:true str with
    | raw when String.length raw = Digestif.SHA1.digest_size ->
        Ok (Carton.Uid.unsafe_of_string raw)
    | _ -> error_msgf "Invalid unique identifier: %S" str
    | exception _ -> error_msgf "Invalid unique identifier: %S" str
  in
  let pp ppf (uid : Carton.Uid.t) =
    Fmt.string ppf (Ohex.encode (uid :> string))
  in
  let uid = Arg.conv (parser, pp) in
  Arg.(non_empty & pos_all uid [] & info [] ~doc ~docv:"UID")

let term =
  let open Term in
  const run $ setup_logs $ threads $ pagesize $ setup_signature $ src $ uids

let cmd : (unit, string) result Cmd.t =
  let doc = "A tool to delete entries from a PACK file." in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "$(tname) produces a new PACK file (and its associated IDX file) from \
         an existing PACK file, with the specified entries removed. Entries \
         that transitively depended (via delta chains) on a removed entry are \
         preserved and rewritten: direct dependents are materialized as bases \
         and deeper descendants keep their delta hunks."
    ]
  in
  let info = Cmd.info "delete" ~doc ~man in
  Cmd.v info term
