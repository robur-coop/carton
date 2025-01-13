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

let deflated_diff source target oc =
  let src = bigstring_of_filename source in
  let dst = bigstring_of_filename target in
  let index = Duff.make src in
  let duff = Duff.delta index ~source:src ~target:dst in
  Logs.debug (fun m -> m "%d hunk(s) generated" (Stdlib.List.length duff));
  Logs.debug (fun m ->
      let saved =
        Stdlib.List.fold_left
          (fun a -> function Duff.Copy (_, len) -> a + len | _ -> a)
          0 duff
      in
      m "%d byte(s) saved" saved);
  (* NOTE(dinosaure): a pretty special case. *)
  begin
    match duff with
    | [ Duff.Insert (0, 0) ] ->
        Fmt.failwith "No difference between the source and the target"
    | _ -> ()
  end;
  let src = Cachet.Bstr.of_bigstring src in
  let dst = Cachet.Bstr.of_bigstring dst in
  let rec output encoder buf = function
    | `Ok -> ()
    | `Partial ->
        let len = Bigarray.Array1.dim buf - Carton.H.N.dst_rem encoder in
        let tmp = Cachet.Bstr.of_bigstring buf in
        let str = Cachet.Bstr.sub_string tmp ~off:0 ~len in
        output_string oc str;
        Carton.H.N.dst encoder buf 0 (Bigarray.Array1.dim buf);
        output encoder buf (Carton.H.N.encode encoder `Await)
  and go encoder buf = function
    | [] ->
        output encoder buf (Carton.H.N.encode encoder `End);
        Ok ()
    | Duff.Copy (off, len) :: rest ->
        let copy = `Copy (off, len) in
        output encoder buf (Carton.H.N.encode encoder copy);
        go encoder buf rest
    | Duff.Insert (off, len) :: rest ->
        let str = Cachet.Bstr.sub_string dst ~off ~len in
        let insert = `Insert str in
        output encoder buf (Carton.H.N.encode encoder insert);
        go encoder buf rest
  in
  let src_len = Cachet.Bstr.length src in
  let dst_len = Cachet.Bstr.length dst in
  let encoder = Carton.H.N.encoder `Manual ~src_len ~dst_len in
  let buf = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x7ff in
  Carton.H.N.dst encoder buf 0 (Bigarray.Array1.dim buf);
  go encoder buf duff

let inflated_diff source target oc =
  let src = bigstring_of_filename source in
  let dst = bigstring_of_filename target in
  let index = Duff.make src in
  let duff = Duff.delta index ~source:src ~target:dst in
  let src_len = Bigarray.Array1.dim src in
  let encoder =
    let i = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000 in
    let q = De.Queue.create 0x100 in
    let w = De.Lz77.make_window ~bits:15 in
    Carton.Zh.N.encoder ~i ~q ~w ~source:src_len dst `Manual duff
  in
  let rec go encoder buf =
    match Carton.Zh.N.encode encoder with
    | `Flush encoder ->
        let len = Bigarray.Array1.dim buf - Carton.Zh.N.dst_rem encoder in
        let tmp = Cachet.Bstr.of_bigstring buf in
        let str = Cachet.Bstr.sub_string tmp ~off:0 ~len in
        output_string oc str;
        go (Carton.Zh.N.dst encoder buf 0 (Bigarray.Array1.dim buf)) buf
    | `End -> Ok ()
  in
  let buf = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000 in
  let encoder = Carton.Zh.N.dst encoder buf 0 (Bigarray.Array1.dim buf) in
  go encoder buf

let run _quiet compressed source target output =
  let oc, finally =
    match output with
    | Some output ->
        let oc = open_out (Fpath.to_string output) in
        let finally () = close_out oc in
        (oc, finally)
    | None -> (stdout, ignore)
  in
  Fun.protect ~finally @@ fun () ->
  if compressed then inflated_diff source target oc
  else deflated_diff source target oc

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
  let doc = "The source used to compress the given $(i,target)." in
  let open Arg in
  required & pos 0 (some existing_file) None & info [] ~doc ~docv:"FILE"

let target =
  let doc = "The target to compress." in
  let open Arg in
  required & pos 1 (some existing_file) None & info [] ~doc ~docv:"FILE"

let output =
  let doc = "The output file to save the patch." in
  let open Arg in
  value & opt (some new_file) None & info [ "o"; "output" ] ~doc ~docv:"FILE"

let compression =
  let doc = "Generate a compressed patch (with zlib)." in
  let open Arg in
  value & flag & info [ "z"; "compression" ] ~doc

let term =
  let open Term in
  const run $ setup_logs $ compression $ source $ target $ output

let cmd : (unit, string) result Cmd.t =
  let doc =
    "A tool to generate a patch between a $(i,source) and a $(i,target)."
  in
  let man = [] in
  let info = Cmd.info "diff" ~doc ~man in
  Cmd.v info term
