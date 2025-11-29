exception Invalid_entry of string

external reraise : exn -> 'a = "%reraise"

let ( $ ) f g x = f (g x)

let kind_of_string = function
  | "a" | "commit" -> `A
  | "b" | "tree" -> `B
  | "c" | "blob" -> `C
  | "d" | "tag" -> `D
  | _ -> invalid_arg "kind_of_string"

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

let entry_of_filename ~identify:(Carton.Identify gen) ~kind filename =
  if Sys.file_exists filename = false || Sys.is_directory filename then
    Fmt.failwith "Invalid source %s" filename;
  let kind = kind_of_string kind in
  let fd = Unix.openfile filename Unix.[ O_RDONLY ] 0o644 in
  let finally () = Unix.close fd in
  Fun.protect ~finally @@ fun () ->
  let stat = Unix.fstat fd in
  let barr =
    Unix.map_file fd Bigarray.char Bigarray.c_layout false
      [| stat.Unix.st_size |]
  in
  let bstr = Bigarray.array1_of_genarray barr in
  let uid =
    let len = Carton.Size.of_int_exn stat.Unix.st_size in
    let ctx = gen.Carton.First_pass.init kind len in
    let ctx = gen.Carton.First_pass.feed bstr ctx in
    gen.Carton.First_pass.serialize ctx
  in
  let meta = (kind, filename) in
  Cartonnage.Entry.make ~kind ~length:stat.Unix.st_size uid meta

let entries_from_ic ~identify ~put ~finally ic =
  let rec go () =
    match input_line ic with
    | exception End_of_file -> put None
    | line -> begin
        match Astring.String.cuts ~sep:" " ~empty:false line with
        | [ (("a" | "b" | "c" | "d") as kind); filename ]
        | [ (("commit" | "tree" | "blob" | "tag") as kind); filename ] -> begin
            try
              let entry = entry_of_filename ~identify ~kind filename in
              put (Some entry); go ()
            with exn -> put None; reraise exn
          end
        | _ -> put None; raise (Invalid_entry line)
      end
  in
  Fun.protect ~finally go

let value_of_filename (kind, filename) =
  let fd = Unix.openfile filename Unix.[ O_RDONLY ] 0o644 in
  let finally () = Unix.close fd in
  Fun.protect ~finally @@ fun () ->
  let stat = Unix.fstat fd in
  let barr =
    Unix.map_file fd Bigarray.char Bigarray.c_layout false
      [| stat.Unix.st_size |]
  in
  let bstr = Bigarray.array1_of_genarray barr in
  Carton.Value.make ~kind bstr

let targets ~ref_length ~dispenser ~put =
  let seq = Seq.of_dispenser dispenser in
  let load _uid = value_of_filename in
  let seq = Carton_miou_unix.delta ~ref_length ~load seq in
  let rec go seq =
    match Seq.uncons seq with
    | Some (target, seq) -> put (Some target); go seq
    | None -> put None
    | exception exn ->
        let bt = Printexc.get_raw_backtrace () in
        Logs.err (fun m ->
            m "Unexpected exception: %S (targets)" (Printexc.to_string exn));
        Logs.err (fun m -> m "%s" (Printexc.raw_backtrace_to_string bt));
        reraise exn
  in
  go seq

let store ?level ?header:with_header ?signature:with_signature ~dispenser put =
  let seq = Seq.of_dispenser dispenser in
  let load _uid = value_of_filename in
  let seq =
    Carton_miou_unix.to_pack ?with_header ?with_signature ?level ~load seq
  in
  let rec go seq =
    match Seq.uncons seq with
    | Some (str, seq) -> put (Some str); go seq
    | None -> put None
    | exception exn ->
        Logs.err (fun m ->
            m "Unexpected exception: %S (store)" (Printexc.to_string exn));
        reraise exn
  in
  go seq

module Stream = struct
  type 'a t = {
      arr: 'a option array
    ; lock: Miou.Mutex.t
    ; mutable read_pos: int
    ; mutable write_pos: int
    ; mutable closed: bool
    ; not_empty_or_closed: Miou.Condition.t
    ; not_full: Miou.Condition.t
  }

  let make length =
    {
      arr= Array.make length None
    ; lock= Miou.Mutex.create ()
    ; read_pos= 0
    ; write_pos= 0
    ; closed= false
    ; not_empty_or_closed= Miou.Condition.create ()
    ; not_full= Miou.Condition.create ()
    }

  let put t data =
    Miou.Mutex.protect t.lock @@ fun () ->
    match (data, t.closed) with
    | None, true -> ()
    | None, false ->
        t.closed <- true;
        Miou.Condition.signal t.not_empty_or_closed
    | Some _, true -> invalid_arg "Stream.put: stream already closed"
    | (Some _ as data), false ->
        while (t.write_pos + 1) mod Array.length t.arr = t.read_pos do
          Miou.Condition.wait t.not_full t.lock
        done;
        t.arr.(t.write_pos) <- data;
        t.write_pos <- (t.write_pos + 1) mod Array.length t.arr;
        Miou.Condition.signal t.not_empty_or_closed

  let get t =
    Miou.Mutex.protect t.lock @@ fun () ->
    while t.write_pos = t.read_pos && not t.closed do
      Miou.Condition.wait t.not_empty_or_closed t.lock
    done;
    if t.write_pos = t.read_pos && t.closed then None
    else begin
      let data = Option.get t.arr.(t.read_pos) in
      t.arr.(t.read_pos) <- None;
      t.read_pos <- (t.read_pos + 1) mod Array.length t.arr;
      Miou.Condition.signal t.not_full;
      Some data
    end

  let to_dispenser t () = get t
end

let bar ~total =
  let open Progress.Line in
  let style = if Fmt.utf_8 Fmt.stdout then `UTF8 else `ASCII in
  list [ brackets @@ bar ~style ~width:(`Fixed 30) total; count_to total ]

let with_reporter ~config ?total quiet =
  match (quiet, total) with
  | true, _ | _, None -> (ignore, ignore)
  | false, Some total ->
      let display =
        Progress.(Display.start ~config Multi.(line (bar ~total)))
      in
      let[@warning "-8"] Progress.Reporter.[ reporter ] =
        Progress.Display.reporters display
      in
      let on n =
        reporter n;
        Progress.Display.tick display
      in
      let finally () = Progress.Display.finalise display in
      (on, finally)

let run quiet progress without_progress header signature without_signature
    entries output =
  let ref_length = Digestif.SHA1.digest_size in
  let identify = Carton.Identify git_identify in
  let ic, ic_finally =
    match entries with
    | Some entries ->
        let ic = open_in (Fpath.to_string entries) in
        let finally () = close_in ic in
        (ic, finally)
    | None -> (stdin, ignore)
  in
  let oc, oc_finally =
    match output with
    | Some output ->
        let oc = open_out output in
        let finally () = close_out oc in
        (oc, finally)
    | None -> (stdout, ignore)
  in
  Miou_unix.run @@ fun () ->
  let q0 = Stream.make 0x100 in
  let q1 = Stream.make 0x100 in
  let p0 =
    Miou.call @@ fun () ->
    entries_from_ic ~identify ~put:(Stream.put q0) ~finally:ic_finally ic
  in
  let p1 =
    Miou.call @@ fun () ->
    targets ~ref_length ~dispenser:(Stream.to_dispenser q0) ~put:(Stream.put q1)
  in
  let p2 =
    Miou.async @@ fun () ->
    let signature = if without_signature then None else Some signature in
    Fun.protect ~finally:oc_finally @@ fun () ->
    let put = Option.iter (output_string oc) in
    let on, finally =
      with_reporter ~config:progress ?total:header (quiet || without_progress)
    in
    Fun.protect ~finally @@ fun () ->
    let dispenser =
      let fn = Stream.to_dispenser q1 in
      fun () ->
        let value = fn () in
        on (Option.fold ~none:0 ~some:(Fun.const 1) value);
        value
    in
    store ?header ?signature ~dispenser put
  in
  let _lst = Miou.await_all [ p0; p1; p2 ] in
  Ok ()

open Cmdliner
open Carton_cli

let number_of_entries =
  let doc =
    "The number of entries to PACK (required if you want to emit the PACK \
     header)."
  in
  let open Arg in
  value & opt (some int) None & info [ "n"; "number" ] ~doc ~docv:"NUMBER"

let existing_file =
  let parser str =
    match Fpath.of_string str with
    | Ok _ as v when Sys.file_exists str && Sys.is_directory str = false -> v
    | Ok v -> error_msgf "%a does not exist" Fpath.pp v
    | Error _ as err -> err
  in
  Arg.conv (parser, Fpath.pp)

let entries =
  let doc = "The file which contains entries to PACK." in
  let open Arg in
  value
  & opt (some existing_file) None
  & info [ "e"; "entries" ] ~doc ~docv:"FILE"

let without_signature =
  let doc = "Don't generate a signature for the generated PACK file." in
  let open Arg in
  value & flag & info [ "without-signature" ] ~doc

let without_progress =
  let doc = "Don't print progress bar." in
  Arg.(value & flag & info [ "without-progress" ] ~doc)

let output =
  let doc = "The PACK file." in
  let parser str =
    match Fpath.of_string str with
    | Ok path ->
        if Sys.file_exists str then error_msgf "%a already exists" Fpath.pp path
        else Ok (Fpath.to_string path)
    | Error _ as err -> err
  in
  let new_file = Arg.conv (parser, Fmt.string) in
  Arg.(value & pos 0 (some new_file) None & info [] ~doc ~docv:"FILE")

let term =
  let open Term in
  const run
  $ setup_logs
  $ setup_progress
  $ without_progress
  $ number_of_entries
  $ setup_signature
  $ without_signature
  $ entries
  $ output

let cmd : (unit, string) result Cmd.t =
  let doc = "A tool to make a PACK file from a list of objects." in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "$(tname) generates a PACK file based on a list of files corresponding \
         to the objects you wish to pack. This list must be in $(b,“type \
         file”) format, where the type can be $(i,A), $(i,B), $(i,C) or $(i,D) \
         (Git-style nominations also work, such as $(i,commit), $(i,tree), \
         $(i,blob) or $(i,tag))."
    ; `P
        "$(tname) can generate a partial PACK file without its header or final \
         signature. It may be useful to parallelize $(i,islands of objects) \
         and concatenate these partial PACK files together and add an \
         appropriate header at the beginning and the signature at the end."
    ; `P
        "If you wish to produce an entire PACK file, you must specify the \
         number of objects it should contain (via the $(b,--number) option)."
    ; `P
        "Objects can be given via standard input or via a file listing the \
         objects."
    ]
  in
  let info = Cmd.info "make" ~doc ~man in
  Cmd.v info term
