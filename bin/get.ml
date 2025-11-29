let error_msgf fmt = Fmt.kstr (fun msg -> Error (`Msg msg)) fmt

let pp_kind ppf = function
  | `A -> Fmt.pf ppf "%a" Fmt.(styled `Blue string) "a"
  | `B -> Fmt.pf ppf "%a" Fmt.(styled `Cyan string) "b"
  | `C -> Fmt.pf ppf "%a" Fmt.(styled `Green string) "c"
  | `D -> Fmt.pf ppf "%a" Fmt.(styled `Magenta string) "d"

type tree = Src of int | Apply of int * tree

let rec pp_sub_tree ppf = function
  | Src offset -> Fmt.pf ppf "Δ %08x" offset
  | Apply (offset, tree) -> Fmt.pf ppf "Δ %08x@\n%a" offset pp_sub_tree tree

and pp_tree ppf = function
  | Apply (offset, tree) -> Fmt.pf ppf "  %08x@\n%a" offset pp_sub_tree tree
  | Src offset -> Fmt.pf ppf "%08x" offset

let path_to_tree path =
  match Carton.Path.to_list path with
  | [] -> assert false
  | base :: rest ->
      let rec go acc = function
        | [] -> acc
        | node :: rest -> go (Apply (node, acc)) rest
      in
      go (Src base) rest

let show_metadata ppf (cache, path, value) =
  let kind = Carton.Value.kind value
  and length = Carton.Value.length value
  and depth = Carton.Value.depth value in
  Fmt.pf ppf "kind:         %a\n%!" pp_kind kind;
  Fmt.pf ppf "length:       %d byte(s)\n%!" length;
  Fmt.pf ppf "depth:        %d\n%!" depth;
  let cache_miss = Cachet.cache_miss cache in
  let cache_hit = Cachet.cache_hit cache in
  Fmt.pf ppf "cache misses: %d\n%!" cache_miss;
  Fmt.pf ppf "cache hits:   %d\n%!" cache_hit;
  Fmt.pf ppf "tree:         @[<h>%a@]\n%!" pp_tree (path_to_tree path);
  Fmt.pf ppf "\n%!"

let map (fd, max) ~pos len =
  let len = Int.min (max - pos) len in
  let pos = Int64.of_int pos in
  let barr =
    Unix.map_file fd ~pos Bigarray.char Bigarray.c_layout false [| len |]
  in
  Miou.yield ();
  Bigarray.array1_of_genarray barr

let on_exn = function
  | Cachet.Out_of_bounds logical_address ->
      error_msgf "%08x is inaccessible, is it a good PACK file?" logical_address
  | Carton.Bad_type ->
      error_msgf "Invalid type of the object, possibly a malformed PACK file"
  | Carton.Too_deep -> error_msgf "Object requested too deep"
  | exn -> error_msgf "Unexpected exception %S" (Printexc.to_string exn)

let protect ~finally ~on_exn fn =
  let finally_no_exn () =
    try finally ()
    with exn ->
      let bt = Printexc.get_raw_backtrace () in
      Printexc.raise_with_backtrace (Fun.Finally_raised exn) bt
  in
  let on_exn_no_exn exn =
    try finally (); on_exn exn
    with exn ->
      let bt = Printexc.get_raw_backtrace () in
      Printexc.raise_with_backtrace (Fun.Finally_raised exn) bt
  in
  match fn () with
  | value -> finally_no_exn (); value
  | exception exn -> on_exn_no_exn exn

let run quiet hxd format_of_output (output_filename, output) without_metadata
    pack identifier =
  Miou_unix.run @@ fun () ->
  let ref_length = Digestif.SHA1.digest_size in
  let hash_length = Digestif.SHA1.digest_size in
  let filename, cursor =
    match (pack, identifier) with
    | `Pack _, `Uid _ ->
        Fmt.failwith
          "Impossible to load an object from its unique identifier without the \
           IDX file"
    | (`Pack filename | `Idx (_, filename)), `Offset cursor -> (filename, cursor)
    | `Idx (idx, filename), `Uid uid ->
        let idx = Fpath.to_string idx in
        let fd, idx =
          let fd = Unix.openfile idx Unix.[ O_RDONLY ] 0o644 in
          let stat = Unix.fstat fd in
          let fd = (fd, stat.Unix.st_size) in
          let length = stat.Unix.st_size in
          (fst fd, Classeur.make ~map fd ~length ~hash_length ~ref_length)
        in
        if String.length uid != ref_length then
          Fmt.failwith "Invalid unique identifier: %s" (Ohex.encode uid);
        let uid = Classeur.uid_of_string idx uid in
        let uid = Result.get_ok uid in
        let finally () = Unix.close fd in
        Fun.protect ~finally @@ fun () ->
        Logs.debug (fun m -> m "Try to find %a" Ohex.pp (uid :> string));
        (filename, Classeur.find_offset idx uid)
  in
  let t = Carton_miou_unix.make ~ref_length filename in
  let fd, _ = Carton.fd t in
  let finally () = Unix.close fd in
  protect ~on_exn ~finally @@ fun () ->
  let path = Carton.path_of_offset t ~cursor in
  let size = Carton.size_of_offset t ~cursor Carton.Size.zero in
  Logs.debug (fun m -> m "allocate a blob of %d byte(s)" (size :> int));
  let blob = Carton.Blob.make ~size in
  let value = Carton.of_offset_with_path t ~path blob ~cursor in
  let str = Carton.Value.string value in
  let cache = Carton.cache t in
  if (not without_metadata) && not quiet then
    show_metadata Fmt.stdout (cache, path, value);
  if not quiet then begin
    match (output_filename, format_of_output) with
    | _, Some `Hex | None, None ->
        Fmt.pf output "@[<hov>%a@]%!" (Hxd_string.pp hxd) str;
        if Option.is_some output_filename then Fmt.pf output "\n%!";
        Ok ()
    | _, Some `Raw | Some _, None -> Fmt.pf output "%s%!" str; Ok ()
  end
  else Ok ()

open Cmdliner
open Carton_cli

let format_of_output =
  let open Arg in
  let hex =
    let doc = "Displaying the object in the hexdump format." in
    info [ "hex" ] ~doc
  in
  let raw = info [ "raw" ] ~doc:"Displaying the object as is." in
  value & vflag None [ (Some `Hex, hex); (Some `Raw, raw) ]

let output =
  let doc = "Save the object into the given file." in
  let parser str =
    if str = "-" then Ok (None, Fmt.stdout)
    else
      match Fpath.of_string str with
      | Ok v when Sys.file_exists str ->
          error_msgf "%a already exists" Fpath.pp v
      | Ok v ->
          let oc = open_out (Fpath.to_string v) in
          at_exit (fun () -> close_out oc);
          Ok (Some v, Format.formatter_of_out_channel oc)
      | Error _ as err -> err
  in
  let pp ppf (v, _) =
    match v with Some v -> Fpath.pp ppf v | None -> Fmt.string ppf "-"
  in
  let open Arg in
  let new_file = conv (parser, pp) in
  value
  & opt new_file (None, Fmt.stdout)
  & info [ "o"; "output" ] ~doc ~docv:"FILE"

let without_metadata =
  let doc = "Don't show metadata of the given PACK object." in
  Arg.(value & flag & info [ "without-metadata" ] ~doc)

let pack =
  let doc =
    "The file used to access to the PACK file (it can be the PACK file \
     directly or the associated IDX file)."
  in
  Arg.(required & pos 0 (some pack) None & info [] ~doc ~docv:"FILE")

let uid_of_string_opt str =
  match Ohex.decode ~skip_whitespace:true str with
  | uid -> Some uid
  | exception _exn -> None

let identifier =
  let doc =
    "The unique identifier of the object or the offset of it into the given \
     PACK file."
  in
  let parser str =
    if String.length str > 1 && str.[0] = '@' then
      let offset = String.sub str 1 (String.length str - 1) in
      match int_of_string_opt offset with
      | Some offset -> Ok (`Offset offset)
      | None -> error_msgf "Invalid position of the object: %S" str
    else
      match (int_of_string_opt str, uid_of_string_opt str) with
      | Some offset, None -> Ok (`Offset offset)
      | _, Some uid -> Ok (`Uid uid)
      | None, None -> error_msgf "Invalid position of the object: %S" str
  in
  let pp ppf = function
    | `Offset offset -> Fmt.pf ppf "0x%x" offset
    | `Uid uid -> Fmt.pf ppf "%s" (Ohex.encode uid)
  in
  let identifier = Arg.conv (parser, pp) in
  let open Arg in
  required & pos 1 (some identifier) None & info [] ~doc ~docv:"IDENTIFIER"

let term =
  let open Term in
  const run
  $ setup_logs
  $ setup_hxd
  $ format_of_output
  $ output
  $ without_metadata
  $ pack
  $ identifier

let term =
  let open Term in
  let error_to_string = function `Msg msg -> msg in
  map (Result.map_error error_to_string) term

let cmd =
  let doc = "A tool to extract objects from the given PACK file." in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "$(tname) extracts an object according to its offset in the given PACK \
         file or its unique identifier according to the associated IDX file. \
         The user can therefore extract the object according to the \
         $(i,*.pack) file or the $(i,*.idx) file. Both files $(b,must) share \
         the same name."
    ; `P
        "The IDX file can be generated by the $(b,index) command. Here's an \
         example of using a PACK file with the $(tname) command."
    ; `Pre
        "\\$ carton index pack.pack\n\
         \\$ carton verify --without-progress -v pack.pack | head -n1\n\
         641adae7aa81471240b8b8d40400fe2ca6760098 commit 12\n\
         \\$ carton get --without-metadata --raw pack.pack @12 > commit.txt\n\
         \\$ carton get --without-metadata --raw pack.pack \
         641adae7aa81471240b8b8d40400fe2ca6760098 > commit.txt"
    ; `P
        "A range of information is available through the object. Its type and \
         the number of patches required to rebuild the object. The number of \
         times Carton had to load new pages, and the object's actual size."
    ; `P
        "The object can be saved to a file if desired (via the $(b,-o) option)."
    ]
  in
  let info = Cmd.info "get" ~doc ~man in
  Cmd.v info term
