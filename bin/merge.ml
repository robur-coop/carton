let ( $ ) f g x = f (g x)

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

let by_kind a b =
  let ka = Cartonnage.Entry.kind a in
  let kb = Cartonnage.Entry.kind b in
  Carton.Kind.compare ka kb

let by_size a b =
  let la = Cartonnage.Entry.length a in
  let lb = Cartonnage.Entry.length b in
  Int.compare la lb

let sort entries =
  let entries = Array.concat entries in
  let lst = Array.to_list entries in
  let set = Hashtbl.create 0x7ff in
  let cnt = ref 0 in
  let fn entry =
    let keep = not (Hashtbl.mem set (Cartonnage.Entry.uid entry)) in
    if keep then Hashtbl.add set (Cartonnage.Entry.uid entry) ();
    if keep then incr cnt;
    keep
  in
  let lst = Stdlib.List.filter fn lst in
  (!cnt, Stdlib.List.to_seq lst)

let sort = { Carton_miou_unix.sort }

let run _quiet threads pagesize digest filenames output =
  Miou_unix.run @@ fun () ->
  let ref_length = Digestif.SHA1.digest_size in
  let identify = Carton.Identify git_identify in
  let cfg = Carton_miou_unix.config ~threads ~pagesize ~ref_length identify in
  let seq = Carton_miou_unix.merge ~cfg ~digest ~sort filenames in
  let oc, oc_finally =
    match output with
    | Some output ->
        let oc = open_out output in
        let finally () = close_out oc in
        (oc, finally)
    | None -> (stdout, ignore)
  in
  Fun.protect ~finally:oc_finally @@ fun () ->
  Seq.iter (output_string oc) seq;
  Ok ()

open Cmdliner
open Carton_cli

let output =
  let doc = "The new PACK file." in
  let parser str =
    match Fpath.of_string str with
    | Ok path ->
        if Sys.file_exists str then error_msgf "%a already exists" Fpath.pp path
        else Ok (Fpath.to_string path)
    | Error _ as err -> err
  in
  let new_file = Arg.conv (parser, Fmt.string) in
  Arg.(value & pos 0 (some new_file) None & info [] ~doc ~docv:"FILE")

let packs =
  let doc = "The PACK file to merge." in
  let parser str =
    match Fpath.of_string str with
    | Ok _ as value when Sys.file_exists str && Sys.is_directory str = false ->
        value
    | Ok value -> error_msgf "%a does not exist" Fpath.pp value
    | Error _ as err -> err
  in
  let existing_file = Arg.conv (parser, Fpath.pp) in
  let open Arg in
  non_empty & opt_all existing_file [] & info [ "p"; "pack" ] ~doc ~docv:"FILE"

let term =
  let open Term in
  const run $ setup_logs $ threads $ pagesize $ setup_signature $ packs $ output

let cmd : (unit, string) result Cmd.t =
  let doc = "A tool to merge several PACK files into one." in
  let man = [] in
  let info = Cmd.info "merge" ~doc ~man in
  Cmd.v info term
