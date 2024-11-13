let error_msgf fmt = Fmt.kstr (fun msg -> Error (`Msg msg)) fmt

let bigstring_of_filename filename =
  let stat = Unix.stat (Fpath.to_string filename) in
  let fd = Unix.openfile (Fpath.to_string filename) Unix.[ O_RDONLY ] 0o644 in
  let finally () = Unix.close fd in
  Fun.protect ~finally @@ fun () ->
  let barr =
    Unix.map_file fd Bigarray.char Bigarray.c_layout false
      [| stat.Unix.st_size |]
  in
  Bigarray.array1_of_genarray barr

let new_target filename ~len =
  let fd =
    Unix.openfile (Fpath.to_string filename)
      Unix.[ O_CREAT; O_RDWR; O_TRUNC ]
      0o644
  in
  let finally () = Unix.close fd in
  Fun.protect ~finally @@ fun () ->
  Unix.ftruncate fd len;
  let barr = Unix.map_file fd Bigarray.char Bigarray.c_layout true [| len |] in
  Bigarray.array1_of_genarray barr

let deflated_apply source ic target =
  let src = bigstring_of_filename source in
  let decoder = Carton.H.M.decoder ~source:src `Manual in
  let buf = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000 in
  let rec go dst =
    match Carton.H.M.decode decoder with
    | `Await ->
        let len = Carton.H.input_bigstring ic buf 0 (Bigarray.Array1.dim buf) in
        Carton.H.M.src decoder buf 0 len;
        go dst
    | `Header (src_len, dst_len) ->
        if src_len != Bigarray.Array1.dim src then invalid_arg "Invalid source";
        Logs.debug (fun m -> m "Load the target (%d byte(s))" dst_len);
        let dst = new_target target ~len:dst_len in
        Carton.H.M.dst decoder dst 0 dst_len;
        go dst
    | `End -> Ok ()
    | `Malformed err -> error_msgf "Malformed patch: %s" err
  in
  go Carton.H.bigstring_empty

let inflated_apply source ic target =
  let src = bigstring_of_filename source in
  let decoder =
    let o = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000 in
    let allocate bits = De.make_window ~bits in
    Carton.Zh.M.decoder ~source:src ~o ~allocate `Manual
  in
  let buf = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000 in
  let rec go dst decoder =
    match Carton.Zh.M.decode decoder with
    | `Await decoder ->
        let len = Carton.H.input_bigstring ic buf 0 (Bigarray.Array1.dim buf) in
        let decoder = Carton.Zh.M.src decoder buf 0 len in
        go dst decoder
    | `Header (src_len, dst_len, decoder) ->
        if src_len != Bigarray.Array1.dim src then invalid_arg "Invalid source";
        let dst = new_target target ~len:dst_len in
        let decoder = Carton.Zh.M.dst decoder dst 0 dst_len in
        go dst decoder
    | `End _ -> Ok ()
    | `Malformed err -> error_msgf "Malformed patch: %s" err
  in
  go Carton.H.bigstring_empty decoder

let run _quiet compressed source patch target =
  let ic, finally =
    match patch with
    | Some patch ->
        let ic = open_in (Fpath.to_string patch) in
        let finally () = close_in ic in
        (ic, finally)
    | None -> (stdin, ignore)
  in
  Fun.protect ~finally @@ fun () ->
  if compressed then inflated_apply source ic target
  else deflated_apply source ic target

open Cmdliner
open Args

let existing_file =
  let parser str =
    match Fpath.of_string str with
    | Ok value ->
        if Sys.file_exists str && Sys.is_directory str = false then Ok value
        else error_msgf "%a is not an existing file" Fpath.pp value
    | Error _ as err -> err
  in
  Arg.conv (parser, Fpath.pp)

let new_file =
  let parser str =
    match Fpath.of_string str with
    | Ok value ->
        if Sys.file_exists str then error_msgf "%a already exist" Fpath.pp value
        else Ok value
    | Error _ as err -> err
  in
  Arg.conv (parser, Fpath.pp)

let source =
  let doc =
    "The source used to reconstruct the target with the given $(i,patch)."
  in
  let open Arg in
  required & pos 0 (some existing_file) None & info [] ~doc ~docv:"FILE"

let target =
  let doc = "The target to produce." in
  let open Arg in
  required & pos 1 (some new_file) None & info [] ~doc ~docv:"FILE"

let patch =
  let doc =
    "The patch (compressed or not) used to reconstruct the $(i,target) with \
     the given $(i,source)."
  in
  let open Arg in
  value
  & opt (some existing_file) None
  & info [ "p"; "patch" ] ~doc ~docv:"FILE"

let compression =
  let doc = "Assert that the given patch is compressed." in
  let open Arg in
  value & flag & info [ "z"; "compression" ] ~doc

let term =
  let open Term in
  const run $ setup_logs $ compression $ source $ patch $ target

let term =
  let open Term in
  let error_to_string = function `Msg msg -> msg in
  map (Result.map_error error_to_string) term

let cmd =
  let doc =
    "A tool to apply a patch and reconstruct a file from the given patch and a \
     source."
  in
  let man = [] in
  let info = Cmd.info "apply" ~doc ~man in
  Cmd.v info term
