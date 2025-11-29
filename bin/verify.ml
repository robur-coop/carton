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

let pp_kind ppf = function
  | `A -> Fmt.string ppf "commit"
  | `B -> Fmt.string ppf "tree  "
  | `C -> Fmt.string ppf "blob  "
  | `D -> Fmt.string ppf "tag   "

let pp_status consumed ~max_consumed ~max_offset ppf = function
  | Carton.Unresolved_base { cursor } -> Fmt.pf ppf "%08x" cursor
  | Unresolved_node _ -> ()
  | Resolved_base { cursor; uid; kind; crc } ->
      Fmt.pf ppf "%a %a %*d %*d %08lx" Carton.Uid.pp uid pp_kind kind max_offset
        cursor max_consumed
        (Hashtbl.find consumed cursor)
        (Optint.to_unsigned_int32 crc)
  | Resolved_node { cursor; uid; kind; crc; depth; parent } ->
      Fmt.pf ppf "%a %a %*d %*d %08lx %2d %a" Carton.Uid.pp uid pp_kind kind
        max_offset cursor max_consumed
        (Hashtbl.find consumed cursor)
        (Optint.to_unsigned_int32 crc)
        (depth - 1) Carton.Uid.pp parent

let pp_status_without_consumed ~max_offset ppf = function
  | Carton.Unresolved_base { cursor } -> Fmt.pf ppf "%08x" cursor
  | Unresolved_node _ -> ()
  | Resolved_base { cursor; uid; kind; crc } ->
      Fmt.pf ppf "%a %a %*d %08lx" Carton.Uid.pp uid pp_kind kind max_offset
        cursor
        (Optint.to_unsigned_int32 crc)
  | Resolved_node { cursor; uid; kind; crc; depth; parent } ->
      Fmt.pf ppf "%a %a %*d %08lx %2d %a" Carton.Uid.pp uid pp_kind kind
        max_offset cursor
        (Optint.to_unsigned_int32 crc)
        (depth - 1) Carton.Uid.pp parent

let bar ~total =
  let open Progress.Line in
  let style = if Fmt.utf_8 Fmt.stdout then `UTF8 else `ASCII in
  list [ brackets @@ bar ~style ~width:(`Fixed 30) total; count_to total ]

let with_reporter ~config quiet t fn =
  let on_entry, on_object, finally =
    match quiet with
    | true -> (ignore, ignore, ignore)
    | false ->
        let lines = Progress.Multi.(line t ++ line t) in
        let display = Progress.Display.start ~config lines in
        let[@warning "-8"] Progress.Reporter.[ reporter0; reporter1 ] =
          Progress.Display.reporters display
        in
        let on_entry n =
          reporter0 n;
          Progress.Display.tick display
        in
        let on_object n =
          reporter1 n;
          Progress.Display.tick display
        in
        let finally () = Progress.Display.finalise display in
        (on_entry, on_object, finally)
  in
  fn (on_entry, on_object, finally)

let printer ~total entries objects progress =
  let rec go total counter =
    Logs.debug (fun m -> m "%d/%d" counter total);
    if counter < total * 2 then begin
      let new_entries = Atomic.exchange entries 0 in
      let new_objects = Atomic.exchange objects 0 in
      let on_entry, on_object, _ = Miou.Lazy.force progress in
      on_entry new_entries;
      on_object new_objects;
      Miou.yield ();
      go total (counter + new_entries + new_objects)
    end
  in
  fun () ->
    let total = Miou.Computation.await_exn total in
    Logs.debug (fun m -> m "%d entries" total);
    go total 0

let display_first_pass ~config quiet t =
  let c = Miou.Computation.create () in
  let consumed = Hashtbl.create 0x7ff in
  let entries = Atomic.make 0 in
  let objects = Atomic.make 0 in
  let progress =
    Miou.Lazy.from_fun @@ fun () ->
    let total = Miou.Computation.await_exn c in
    with_reporter ~config quiet (t ~total) Fun.id
  in
  let on_entry ~max entry =
    if Miou.Computation.is_running c then
      ignore (Miou.Computation.try_return c max);
    Hashtbl.add consumed entry.Carton_miou_unix.offset entry.consumed;
    Atomic.incr entries;
    Miou.yield ()
  in
  let on_object ~cursor:_ _ _ = Atomic.incr objects; Miou.yield () in
  let printer = Miou.async (printer ~total:c entries objects progress) in
  let finally () =
    let _, _, finally = Miou.Lazy.force progress in
    finally (); Miou.cancel printer
  in
  (on_entry, on_object, finally, consumed)

let run quiet progress without_progress threads pagesize digest without_consumed
    pack =
  Miou_unix.run ~domains:threads @@ fun () ->
  let ref_length = Digestif.SHA1.digest_size in
  let identify = Carton.Identify git_identify in
  let on_entry, on_object, finally, consumed =
    display_first_pass ~config:progress (quiet || without_progress) bar
  in
  let cfg =
    Carton_miou_unix.config ~threads ~pagesize ~ref_length ~on_entry ~on_object
      identify
  in
  let matrix, _hash =
    Fun.protect ~finally @@ fun () ->
    match pack with
    | `Pack filename -> Carton_miou_unix.verify_from_pack ~cfg ~digest filename
    | `Idx (filename, _) ->
        Carton_miou_unix.verify_from_idx ~cfg ~digest filename
  in
  Logs.debug (fun m -> m "%d object(s) verified" (Array.length matrix));
  let max_consumed = Hashtbl.fold (fun _ a b -> Int.max a b) consumed 0 in
  let max_consumed = Float.to_int (log10 (Float.of_int max_consumed)) + 1 in
  let max_offset =
    Array.fold_left
      (fun a -> function
        | Carton.Unresolved_base { cursor } -> Int.max cursor a
        | Unresolved_node _ -> a
        | Resolved_base { cursor; _ } -> Int.max cursor a
        | Resolved_node { cursor; _ } -> Int.max cursor a)
      0 matrix
  in
  let max_offset = Float.to_int (log10 (Float.of_int max_offset)) + 1 in
  let pp_status =
    if without_consumed then pp_status_without_consumed ~max_offset
    else pp_status consumed ~max_consumed ~max_offset
  in
  if not quiet then Array.iter (Fmt.pr "%a\n%!" pp_status) matrix;
  Ok ()

open Cmdliner
open Carton_cli

let pack =
  let doc =
    "The file used to access to the PACK file (it can be the PACK file \
     directly or the associated IDX file)."
  in
  Arg.(required & pos 0 (some pack) None & info [] ~doc ~docv:"FILE")

let without_progress =
  let doc = "Don't print progress bars." in
  Arg.(value & flag & info [ "without-progress" ] ~doc)

let without_consumed =
  let doc = "Don't print consumed bytes by entries." in
  Arg.(value & flag & info [ "without-consumed" ] ~doc)

let term =
  let open Term in
  const run
  $ setup_logs
  $ setup_progress
  $ without_progress
  $ threads
  $ pagesize
  $ setup_signature
  $ without_consumed
  $ pack

let cmd : (unit, string) result Cmd.t =
  let doc = "A tool to verify the given PACK file." in
  let man = [] in
  let info = Cmd.info "verify" ~doc ~man in
  Cmd.v info term
