let src = Logs.Src.create "carton-miou-unix"

module Log = (val Logs.src_log src : Logs.LOG)

external getpagesize : unit -> int = "carton_miou_unix_getpagesize" [@@noalloc]

let failwithf fmt = Format.kasprintf failwith fmt
let ignore3 ~cursor:_ _ _ = ()
let ignorem ~max:_ _ = ()

let bigstring_copy bstr =
  let len = Bigarray.Array1.dim bstr in
  let res = Bigarray.Array1.create Bigarray.char Bigarray.c_layout len in
  Cachet.memcpy bstr ~src_off:0 res ~dst_off:0 ~len;
  res

type file_descr = Unix.file_descr * int

let map (fd, max) ~pos len =
  let len = Int.min (max - pos) len in
  let pos = Int64.of_int pos in
  Log.debug (fun m -> m "map pos:%Lx %d" pos len);
  let barr =
    Unix.map_file fd ~pos Bigarray.char Bigarray.c_layout false [| len |]
  in
  Miou.yield ();
  Bigarray.array1_of_genarray barr

let _max_int31 = 2147483647L
let _max_int63 = 9223372036854775807L
let never uid = failwithf "Impossible to find the object %a" Carton.Uid.pp uid

(** makers *)

let index ?(pagesize = getpagesize ()) ?cachesize ~hash_length ~ref_length
    filename =
  let filename = Fpath.to_string filename in
  let fd = Unix.openfile filename Unix.[ O_RDONLY ] 0o644 in
  let stat = Unix.LargeFile.fstat fd in
  if Sys.word_size = 32 && stat.Unix.LargeFile.st_size > _max_int31 then
    failwith "Too huge IDX file";
  if stat.Unix.LargeFile.st_size > _max_int63 then failwith "Too huge IDX file";
  let fd = (fd, Int64.to_int stat.Unix.LargeFile.st_size) in
  Classeur.make ~pagesize ?cachesize ~map fd
    ~length:(Int64.to_int stat.Unix.LargeFile.st_size)
    ~hash_length ~ref_length

let make ?(pagesize = getpagesize ()) ?cachesize ?z ~ref_length ?(index = never)
    filename =
  let filename = Fpath.to_string filename in
  let fd = Unix.openfile filename Unix.[ O_RDONLY ] 0o644 in
  let stat = Unix.LargeFile.fstat fd in
  if Sys.word_size = 32 && stat.Unix.LargeFile.st_size > _max_int31 then
    failwith "Too huge PACK file";
  if stat.Unix.LargeFile.st_size > _max_int63 then failwith "Too huge PACK file";
  let fd = (fd, Int64.to_int stat.Unix.LargeFile.st_size) in
  let z =
    match z with None -> De.bigstring_create De.io_buffer_size | Some z -> z
  in
  let allocate bits = De.make_window ~bits in
  Carton.make ~pagesize ?cachesize ~map fd ~z ~allocate ~ref_length index

(** some types *)

type entry = { offset: int; crc: Optint.t; consumed: int; size: int }

type config = {
    threads: int option
  ; pagesize: int option
  ; cachesize: int option
  ; ref_length: int
  ; identify: Carton.identify
  ; on_entry: max:int -> entry -> unit
  ; on_object: cursor:int -> Carton.Value.t -> Carton.Uid.t -> unit
}

let _min_threads = Int.min 4 (Stdlib.Domain.recommended_domain_count ())

let config ?(threads = _min_threads) ?(pagesize = getpagesize ()) ?cachesize
    ?(on_entry = ignorem) ?(on_object = ignore3) ~ref_length identify =
  {
    threads= Some threads
  ; pagesize= Some pagesize
  ; cachesize
  ; ref_length
  ; identify
  ; on_entry
  ; on_object
  }

type base = { value: Carton.Value.t; uid: Carton.Uid.t; depth: int }

(** the core of the verification *)

let identify (Carton.Identify gen) ~kind ~len bstr =
  let ctx = gen.Carton.First_pass.init kind (Carton.Size.of_int_exn len) in
  let ctx = gen.Carton.First_pass.feed (Bigarray.Array1.sub bstr 0 len) ctx in
  gen.Carton.First_pass.serialize ctx

let rec resolve_tree ?(on = ignore3) t oracle matrix ~base = function
  | [||] -> ()
  | [| cursor |] ->
      Log.debug (fun m -> m "resolve node at %08x" cursor);
      Log.debug (fun m ->
          m "blob: %d byte(s)"
            (Carton.(Blob.size (Value.blob base.value)) :> int));
      let value = Carton.of_offset_with_source t base.value ~cursor in
      let len = Carton.Value.length value
      and bstr = Carton.Value.bigstring value
      and kind = Carton.Value.kind value in
      let uid = identify oracle.Carton.identify ~kind ~len bstr
      and pos = oracle.where ~cursor
      and crc = oracle.checksum ~cursor
      and depth = succ base.depth in
      on ~cursor value uid;
      matrix.(pos) <-
        Carton.Resolved_node { cursor; uid; crc; kind; depth; parent= base.uid };
      let children = oracle.children ~cursor ~uid in
      let children = Array.of_list children in
      let value = Carton.Value.flip value in
      let base = { value; uid; depth } in
      resolve_tree ~on t oracle matrix ~base children
  | cursors ->
      let source = Carton.Value.source base.value in
      let source = bigstring_copy source in
      let rec go idx =
        if idx < Array.length cursors then begin
          let cursor = cursors.(idx) in
          Log.debug (fun m -> m "resolve node at %08x" cursor);
          Log.debug (fun m ->
              m "blob: %d byte(s)"
                (Carton.(Blob.size (Value.blob base.value)) :> int));
          let dirty = Carton.Value.source base.value in
          let src = Carton.Value.with_source ~source base.value in
          let value = Carton.of_offset_with_source t src ~cursor in
          let len = Carton.Value.length value
          and bstr = Carton.Value.bigstring value
          and kind = Carton.Value.kind value in
          let uid = identify oracle.Carton.identify ~kind ~len bstr
          and pos = oracle.where ~cursor
          and crc = oracle.checksum ~cursor
          and depth = succ base.depth in
          on ~cursor value uid;
          matrix.(pos) <-
            Resolved_node { cursor; uid; crc; kind; depth; parent= base.uid };
          let children = oracle.children ~cursor ~uid in
          Log.debug (fun m ->
              m "resolve children of %08x %a" cursor Carton.Uid.pp uid);
          let children = Array.of_list children in
          let value = Carton.Value.with_source ~source:dirty value in
          let value = Carton.Value.flip value in
          let base = { value; uid; depth } in
          resolve_tree ~on t oracle matrix ~base children;
          go (succ idx)
        end
      in
      go 0

let is_unresolved_base = function
  | Carton.Unresolved_base _ -> true
  | _ -> false

let verify ?(threads = 4) ?(on = ignore3) t oracle matrix =
  let mutex = Miou.Mutex.create () in
  let idx = Atomic.make 0 in
  let rec fn t =
    let pos =
      Miou.Mutex.protect mutex @@ fun () ->
      while
        Atomic.get idx < Array.length matrix
        && is_unresolved_base matrix.(Atomic.get idx) = false
      do
        Atomic.incr idx
      done;
      Atomic.fetch_and_add idx 1
    in
    if pos < Array.length matrix then begin
      let[@warning "-8"] (Carton.Unresolved_base { cursor }) = matrix.(pos) in
      let size = oracle.Carton.size ~cursor in
      Log.debug (fun m -> m "resolve base (object %d) at %08x" pos cursor);
      Log.debug (fun m -> m "allocate a blob of %d byte(s)" (size :> int));
      let blob = Carton.Blob.make ~size in
      let value = Carton.of_offset t blob ~cursor in
      let len = Carton.Value.length value
      and bstr = Carton.Value.bigstring value
      and kind = Carton.Value.kind value in
      let uid = identify oracle.Carton.identify ~kind ~len bstr
      and crc = oracle.checksum ~cursor in
      on ~cursor value uid;
      matrix.(pos) <- Resolved_base { cursor; uid; crc; kind };
      let children = oracle.children ~cursor ~uid in
      let children = Array.of_list children in
      let base = Carton.{ value= Value.flip value; uid; depth= 1 } in
      resolve_tree ~on t oracle matrix ~base children;
      fn t
    end
  in
  let init _thread = Carton.copy t in
  let results = Miou.parallel fn (List.init threads init) in
  List.iter (function Ok () -> () | Error exn -> raise exn) results

(** utils *)

let seq_of_filename filename =
  let ic = open_in (Fpath.to_string filename) in
  let buf = Bytes.create 0x1000 in
  let dispenser () =
    match input ic buf 0 (Bytes.length buf) with
    | 0 -> close_in ic; None
    | len -> Some (Bytes.sub_string buf 0 len)
    | exception End_of_file -> close_in ic; None
  in
  Seq.of_dispenser dispenser

(** compile metadatas from pack file *)

let compile ?(on = ignorem) ~identify ~digest_length seq =
  let children_by_offset = Hashtbl.create 0x7ff in
  let children_by_uid : (Carton.Uid.t, int list) Hashtbl.t =
    Hashtbl.create 0x7ff
  in
  let sizes : (int, Carton.Size.t ref) Hashtbl.t = Hashtbl.create 0x7ff in
  let where = Hashtbl.create 0x7ff in
  let crcs = Hashtbl.create 0x7ff in
  let is_base = Hashtbl.create 0x7ff in
  let index = Hashtbl.create 0x7ff in
  let ref_index = Hashtbl.create 0x7ff in
  let is_thin = ref false in
  let hash = ref (String.make digest_length '\000') in
  let update_size ~parent offset (size : Carton.Size.t) =
    Log.debug (fun m ->
        m "Update the size of %08x (parent: %08x) to %d byte(s)" offset parent
          (size :> int));
    let cell : Carton.Size.t ref = Hashtbl.find sizes parent in
    (cell := Carton.Size.(max !cell size));
    Hashtbl.replace sizes offset cell
  in
  let new_child ~parent child =
    match parent with
    | `Ofs parent -> begin
        match Hashtbl.find_opt children_by_offset parent with
        | None -> Hashtbl.add children_by_offset parent [ child ]
        | Some offsets ->
            Hashtbl.replace children_by_offset parent (child :: offsets)
      end
    | `Ref parent -> begin
        match Hashtbl.find_opt children_by_uid parent with
        | None -> Hashtbl.add children_by_uid parent [ child ]
        | Some offsets ->
            Hashtbl.replace children_by_uid parent (child :: offsets)
      end
  in
  let number_of_objects = ref 0 in
  let fn pos = function
    | `Number n -> number_of_objects := n
    | `Hash value -> hash := value
    | `Entry entry -> begin
        let pos = pred pos in
        let offset = entry.Carton.First_pass.offset in
        let size = entry.Carton.First_pass.size in
        let crc = entry.Carton.First_pass.crc in
        let consumed = entry.Carton.First_pass.consumed in
        on ~max:!number_of_objects { offset; crc; consumed; size:> int };
        Hashtbl.add where offset pos;
        Hashtbl.add crcs offset crc;
        match entry.Carton.First_pass.kind with
        | Carton.First_pass.Base (_, uid) ->
            Hashtbl.add sizes offset (ref size);
            Hashtbl.add is_base pos offset;
            Hashtbl.add index uid offset
        | Ofs { sub; source; target } ->
            Log.debug (fun m ->
                m "new OBJ_OFS object at %08x (rel: %08x)" offset sub);
            let abs_parent = offset - sub in
            update_size ~parent:abs_parent offset
              (Carton.Size.max target source);
            Log.debug (fun m ->
                m "new child for %08x at %08x" abs_parent offset);
            new_child ~parent:(`Ofs abs_parent) offset
        | Ref { ptr; source; target; _ } ->
            Log.debug (fun m ->
                m
                  "new OBJ_REF object at %08x (ptr: %a) (source: %d, target: \
                   %d)"
                  offset Carton.Uid.pp ptr
                  (source :> int)
                  (target :> int));
            begin
              match Hashtbl.find_opt index ptr with
              | Some parent ->
                  update_size ~parent offset (Carton.Size.max source target)
              | None ->
                  Hashtbl.add sizes offset (ref (Carton.Size.max source target))
            end;
            Hashtbl.add ref_index offset ptr;
            new_child ~parent:(`Ref ptr) offset
      end
  in
  Seq.iteri fn seq;
  Hashtbl.iter
    (fun offset ptr ->
      match Hashtbl.find_opt index ptr with
      | Some parent ->
          Log.debug (fun m ->
              m "Update the size of %08x (parent: %08x, %a)" offset parent
                Carton.Uid.pp ptr);
          update_size ~parent offset !(Hashtbl.find sizes offset)
      | None -> ())
    ref_index;
  Log.debug (fun m -> m "%d object(s)" !number_of_objects);
  let children ~cursor ~uid =
    match
      ( Hashtbl.find_opt children_by_offset cursor
      , Hashtbl.find_opt children_by_uid uid )
    with
    | Some (_ :: _ as children), (Some [] | None) -> children
    | (Some [] | None), Some (_ :: _ as children) -> children
    | (None | Some []), (None | Some []) -> []
    | Some lst0, Some lst1 ->
        List.(sort_uniq Int.compare (rev_append lst0 lst1))
  in
  let where ~cursor = Hashtbl.find where cursor in
  let size ~cursor = !(Hashtbl.find sizes cursor) in
  let checksum ~cursor = Hashtbl.find crcs cursor in
  let is_base ~pos = Hashtbl.find_opt is_base pos in
  {
    Carton.identify
  ; children
  ; where
  ; size
  ; checksum
  ; is_base
  ; is_thin= !is_thin
  ; number_of_objects= !number_of_objects
  ; hash= !hash
  }

let verify_from_pack
    ~cfg:
      {
        threads
      ; pagesize
      ; cachesize
      ; ref_length
      ; identify= Carton.Identify gen as identify
      ; on_entry
      ; on_object
      } ~digest filename =
  let z = De.bigstring_create De.io_buffer_size in
  let seq = seq_of_filename filename in
  let seq =
    let window = De.make_window ~bits:15 in
    let allocate _bits = window in
    Carton.First_pass.of_seq ~output:z ~allocate ~ref_length ~digest
      ~identify:gen seq
  in
  let (Carton.First_pass.Digest ({ length= digest_length; _ }, _)) = digest in
  let oracle = compile ~on:on_entry ~identify ~digest_length seq in
  let t = make ?pagesize ?cachesize ~z ~ref_length filename in
  let fd, _ = Carton.fd t in
  let finally () = Unix.close fd in
  Fun.protect ~finally @@ fun () ->
  let matrix =
    Array.init oracle.Carton.number_of_objects @@ fun pos ->
    match oracle.is_base ~pos with
    | Some cursor -> Carton.Unresolved_base { cursor }
    | None -> Unresolved_node
  in
  verify ?threads ~on:on_object t oracle matrix;
  (matrix, oracle.hash)

(** generate entries to pack from a pack file *)

type delta = { source: Carton.Uid.t; depth: int; raw: Cachet.Bstr.t }

let entries_of_pack ~cfg ~digest filename =
  let raw = Hashtbl.create 0x7ff in
  let (Identify gen) = cfg.identify in
  let z = De.bigstring_create De.io_buffer_size in
  let t =
    make ?pagesize:cfg.pagesize ?cachesize:cfg.cachesize ~z
      ~ref_length:cfg.ref_length filename
  in
  let _fd, _ = Carton.fd t in
  let finally () = (* Unix.close fd *) () in
  Fun.protect ~finally @@ fun () ->
  let on ~max:_ entry =
    let cursor = entry.offset and consumed = entry.consumed in
    let bstr = Carton.map t ~cursor ~consumed in
    Hashtbl.add raw cursor bstr
  in
  let seq = seq_of_filename filename in
  let seq =
    let window = De.make_window ~bits:15 in
    let allocate _bits = window in
    Carton.First_pass.of_seq ~output:z ~allocate ~ref_length:cfg.ref_length
      ~digest ~identify:gen seq
  in
  let (Carton.First_pass.Digest ({ length= digest_length; _ }, _)) = digest in
  let oracle = compile ~on ~identify:cfg.identify ~digest_length seq in
  let matrix =
    Array.init oracle.Carton.number_of_objects @@ fun pos ->
    match oracle.is_base ~pos with
    | Some cursor -> Carton.Unresolved_base { cursor }
    | None -> Unresolved_node
  in
  let size = Hashtbl.create 0x7ff in
  let on ~cursor:_ value uid =
    Hashtbl.add size uid (Carton.Value.length value)
  in
  verify ?threads:cfg.threads ~on t oracle matrix;
  let idx = Hashtbl.create 0x7ff in
  let t = Carton.with_index t (Hashtbl.find idx) in
  let fn = function
    | Carton.Unresolved_base _ -> assert false
    | Unresolved_node -> assert false
    | Resolved_base { cursor; uid; kind; _ } ->
        Hashtbl.add idx uid cursor;
        let length = Hashtbl.find size uid in
        let meta = (t, None) in
        Cartonnage.Entry.make ~kind uid ~length:(length :> int) meta
    | Resolved_node { cursor; uid; kind; depth; parent; _ } ->
        Hashtbl.add idx uid cursor;
        let raw = Hashtbl.find raw cursor in
        let delta = { source= parent; depth; raw } in
        let meta = (t, Some delta) in
        let length = Hashtbl.find size uid in
        Cartonnage.Entry.make ~kind uid ~length:(length :> int) meta
  in
  Array.map fn matrix

(** compile metadatas from idx file *)

module Heap = struct
  type t = { mutable arr: int array; mutable len: int }

  let create len = { len= -len; arr= [||] }

  let add t cursor =
    if t.len < 0 then begin
      t.arr <- Array.make (-t.len) cursor;
      t.len <- 0
    end;
    let rec move_up i =
      let fi = (i - 1) / 2 in
      if i > 0 && Int.compare t.arr.(fi) cursor > 0 then begin
        t.arr.(i) <- t.arr.(fi);
        move_up fi
      end
      else t.arr.(i) <- cursor
    in
    move_up t.len;
    t.len <- t.len + 1

  let rec move_down arr n i x =
    let j = (2 * i) + 1 in
    if j < n then
      let j =
        let j' = j + 1 in
        if j' < n && Int.compare arr.(j') arr.(j) < 0 then j' else j
      in
      if Int.compare arr.(j) x < 0 then begin
        arr.(i) <- arr.(j);
        move_down arr n j x
      end
      else arr.(i) <- x
    else arr.(i) <- x

  let remove t =
    let r = t.arr.(0) in
    let n = t.len - 1 in
    t.len <- n;
    let arr = t.arr in
    let x = arr.(n) in
    move_down arr n 0 x; r
end

let rec binary_search arr off len offset =
  if len == 1 then arr.(off)
  else begin
    let m = off + (len / 2) in
    if arr.(m) == offset then m
    else if Int.compare arr.(m) offset > 0 then
      binary_search arr off (m - off) offset
    else binary_search arr m (len - (m - off)) offset
  end

let binary_search arr offset = binary_search arr 0 (Array.length arr) offset

let compile ?(on = ignorem) ~identify pack idx =
  let children = Hashtbl.create 0x7ff in
  let sizes : (int, Carton.Size.t ref) Hashtbl.t = Hashtbl.create 0x7ff in
  let crcs = Hashtbl.create 0x7ff in
  let is_base = Hashtbl.create 0x7ff in
  let number_of_objects = Classeur.max idx in
  let positions = Heap.create number_of_objects in
  let update_size ~chain (size : Carton.Size.t) =
    let base = List.hd (List.rev chain) in
    let cell =
      match Hashtbl.find_opt sizes base with
      | None -> ref size
      | Some cell ->
          cell := Carton.Size.max !cell size;
          cell
    in
    let fn offset =
      let opt = Hashtbl.find_opt sizes offset in
      let cell =
        Option.fold ~none:cell
          ~some:(fun cell' ->
            cell := Carton.Size.max !cell' !cell;
            cell)
          opt
      in
      Hashtbl.replace sizes offset cell
    in
    Log.debug (fun m ->
        m "size of the blob for %08x (as a base): %d"
          (List.hd (List.rev chain))
          (size :> int));
    List.iter fn chain
  in
  let new_child ~parent child =
    match Hashtbl.find_opt children parent with
    | None -> Hashtbl.add children parent [ child ]
    | Some offsets -> Hashtbl.replace children parent (child :: offsets)
  in
  let fn ~(uid : Classeur.uid) ~crc ~offset =
    Log.debug (fun m -> m "on %s" (Ohex.encode (uid :> string)));
    let consumed = 0 and size = 0 in
    on ~max:number_of_objects { offset; crc; consumed; size };
    Heap.add positions offset;
    Hashtbl.add crcs offset crc;
    let path = Carton.path_of_offset pack ~cursor:offset in
    match Carton.Path.to_list path with
    | [] -> assert false
    | [ _cursor ] ->
        update_size ~chain:[ offset ] (Carton.Path.size path);
        Hashtbl.add is_base offset ()
    | _cursor :: parent :: _ as chain ->
        Log.debug (fun m ->
            m "size of the blob for %08x (as a base): %d"
              (List.hd (List.rev chain))
              (Carton.Path.size path :> int));
        update_size ~chain (Carton.Path.size path);
        new_child ~parent offset
  in
  Classeur.iter ~fn idx;
  let positions =
    let arr = Array.make number_of_objects 0 in
    for i = 0 to number_of_objects - 1 do
      arr.(i) <- Heap.remove positions
    done;
    arr
  in
  let children ~cursor ~uid:_ =
    Option.value ~default:[] (Hashtbl.find_opt children cursor)
  in
  let where ~cursor = binary_search positions cursor in
  let size ~cursor = !(Hashtbl.find sizes cursor) in
  let checksum ~cursor = Hashtbl.find crcs cursor in
  let is_base ~pos =
    if Hashtbl.mem is_base positions.(pos) then Some positions.(pos) else None
  in
  {
    Carton.identify
  ; children
  ; where
  ; size
  ; checksum
  ; is_base
  ; is_thin= false
  ; number_of_objects
  ; hash= Classeur.pack idx
  }

let verify_from_idx
    ~cfg:
      {
        threads
      ; pagesize
      ; cachesize
      ; ref_length
      ; identify
      ; on_entry
      ; on_object
      } ~digest filename =
  if Sys.file_exists (Fpath.to_string (Fpath.set_ext ".pack" filename)) = false
  then
    failwith
      "Impossible to find the PACK file associated with the given IDX file";
  let (Carton.First_pass.Digest ({ length= hash_length; _ }, _)) = digest in
  let fd0, idx =
    let fd = Unix.openfile (Fpath.to_string filename) Unix.[ O_RDONLY ] 0o644 in
    let stat = Unix.fstat fd in
    let fd = (fd, stat.Unix.st_size) in
    let idx =
      Classeur.make ?pagesize ?cachesize ~map fd ~length:stat.Unix.st_size
        ~hash_length ~ref_length
    in
    (fst fd, idx)
  in
  let pack =
    let z = De.bigstring_create De.io_buffer_size in
    let index (uid : Carton.Uid.t) =
      let uid = Classeur.uid_of_string_exn idx (uid :> string) in
      Classeur.find_offset idx uid
    in
    make ?pagesize ?cachesize ~z ~ref_length ~index
      (Fpath.set_ext ".pack" filename)
  in
  let oracle = Miou.call @@ fun () -> compile ~on:on_entry ~identify pack idx in
  let oracle = Miou.await_exn oracle in
  let fd1, _ = Carton.fd pack in
  let finally () = Unix.close fd0; Unix.close fd1 in
  Fun.protect ~finally @@ fun () ->
  let matrix =
    Array.init oracle.Carton.number_of_objects @@ fun pos ->
    match oracle.is_base ~pos with
    | Some cursor -> Carton.Unresolved_base { cursor }
    | None -> Unresolved_node
  in
  verify ?threads ~on:on_object pack oracle matrix;
  (matrix, oracle.hash)

(** packer *)

let _max_depth = 50

module Window = struct
  type 'meta t = {
      arr: 'meta Cartonnage.Source.t array
    ; mutable rd_pos: int
    ; mutable wr_pos: int
  }

  let make () = { arr= Array.make 0x100 (Obj.magic ()); rd_pos= 0; wr_pos= 0 }

  (* let is_empty { rd_pos; wr_pos; _ } = rd_pos = wr_pos *)
  let is_full { rd_pos; wr_pos; arr } = wr_pos - rd_pos = Array.length arr
end

let should_we_apply ~ref_length:_ ~source entry =
  let open Cartonnage in
  let size_guessed =
    match Target.patch entry with
    | None -> Target.length entry / 3
    | Some patch -> Patch.length patch
  in
  if Source.length source < Target.length entry then false
  else
    let diff = Source.length source - Target.length entry in
    diff < size_guessed

let apply ~ref_length ~load ~window:t entry =
  let len = t.Window.wr_pos - t.Window.rd_pos in
  let msk = Array.length t.Window.arr - 1 in
  let uid = Cartonnage.Target.uid entry
  and meta = Cartonnage.Target.meta entry in
  let target = Lazy.from_fun (fun () -> load uid meta) in
  for i = 0 to len - 1 do
    let source = t.Window.arr.((t.Window.rd_pos + i) land msk) in
    if
      Cartonnage.Source.depth source < 50
      && should_we_apply ~ref_length ~source entry
    then Cartonnage.Target.diff entry ~source ~target:(Lazy.force target)
  done;
  if Lazy.is_val target || Cartonnage.Target.depth entry == 1 then
    Some (Lazy.force target)
  else None

let append ~window:t source =
  let open Window in
  let msk = Array.length t.arr - 1 in
  match Array.length t.arr - (t.wr_pos - t.rd_pos) with
  | 0 ->
      t.arr.(t.rd_pos land msk) <- source;
      t.rd_pos <- t.rd_pos + 1;
      t.wr_pos <- t.wr_pos + 1
  | _ ->
      t.arr.(t.wr_pos land msk) <- source;
      t.wr_pos <- t.wr_pos + 1

let delta ~ref_length ~load =
  let windows = Array.init 4 (fun _ -> Window.make ()) in
  Seq.map @@ fun entry ->
  let entry = Cartonnage.Target.make entry in
  let k = Carton.Kind.to_int (Cartonnage.Target.kind entry) in
  let target = apply ~ref_length ~load ~window:windows.(k) entry in
  begin
    match (target, not (Window.is_full windows.(k))) with
    | None, false -> ()
    | None, true ->
        if Cartonnage.Target.depth entry < 50 then begin
          let uid = Cartonnage.Target.uid entry
          and meta = Cartonnage.Target.meta entry in
          let target = load uid meta in
          let source = Cartonnage.Target.to_source entry ~target in
          Log.debug (fun m ->
              m "add %a as a possible source (depth: %d)" Carton.Uid.pp
                (Cartonnage.Source.uid source)
                (Cartonnage.Target.depth entry));
          append ~window:windows.(k) source
        end
    | Some target, _ ->
        if Cartonnage.Target.depth entry < 50 then begin
          let source = Cartonnage.Target.to_source entry ~target in
          Log.debug (fun m ->
              m "add %a as a possible source (depth: %d)" Carton.Uid.pp
                (Cartonnage.Source.uid source)
                (Cartonnage.Target.depth entry));
          append ~window:windows.(k) source
        end
  end;
  entry

type ctx = {
    where: (Carton.Uid.t, int) Hashtbl.t
  ; buffers: Cartonnage.buffers
  ; signature: Carton.First_pass.digest option
}

let digest str (Carton.First_pass.Digest (({ feed_bytes; _ } as hash), ctx)) =
  let len = String.length str in
  let ctx = feed_bytes (Bytes.unsafe_of_string str) ~off:0 ~len ctx in
  Carton.First_pass.Digest (hash, ctx)

let to_pack ?with_header ?with_signature ?cursor ?level ~load targets =
  let ctx =
    let o = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000
    and i = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000
    and q = De.Queue.create 0x1000
    and w = De.Lz77.make_window ~bits:15 in
    let buffers = Cartonnage.{ o; i; q; w } in
    let where = Hashtbl.create 0x100 in
    { where; buffers; signature= with_signature }
  in
  let rec go ctx targets encoder cursor =
    let dst = ctx.buffers.o in
    match Cartonnage.Encoder.encode ~o:dst encoder with
    | `Flush (encoder, len) ->
        let bstr = Cachet.Bstr.of_bigstring dst in
        let str = Cachet.Bstr.sub_string bstr ~off:0 ~len in
        let signature = Option.map (digest str) ctx.signature in
        let encoder =
          Cartonnage.Encoder.dst encoder dst 0 (Bigarray.Array1.dim dst)
        in
        let ctx = { ctx with signature } in
        let next () = go ctx targets encoder (cursor + len) in
        Seq.Cons (str, next)
    | `End -> consume ctx targets cursor
  and consume ctx targets cursor =
    match Seq.uncons targets with
    | None -> begin
        match ctx.signature with
        | Some Carton.First_pass.(Digest ({ serialize; _ }, ctx)) ->
            let signature = serialize ctx in
            Seq.Cons (signature, Fun.const Seq.Nil)
        | None -> Seq.Nil
      end
    | Some (entry, targets) ->
        let uid = Cartonnage.Target.uid entry
        and meta = Cartonnage.Target.meta entry in
        let target = load uid meta in
        let buffers = ctx.buffers in
        let where = Hashtbl.find_opt ctx.where in
        Log.debug (fun m ->
            m "encode %a at %08x" Carton.Uid.pp
              (Cartonnage.Target.uid entry)
              cursor);
        let _hdr_len, encoder =
          Cartonnage.encode ?level ~buffers ~where entry ~target ~cursor
        in
        Hashtbl.add ctx.where (Cartonnage.Target.uid entry) cursor;
        go ctx targets encoder cursor
  in
  let hdr =
    match with_header with
    | None -> None
    | Some n ->
        let hdr = Bytes.make 12 '\000' in
        Bytes.set_int32_be hdr 0 0x5041434bl;
        Bytes.set_int32_be hdr 4 2l;
        Bytes.set_int32_be hdr 8 (Int32.of_int n);
        Some (Bytes.unsafe_to_string hdr)
  in
  let cursor =
    match (cursor, hdr) with
    | None, Some _ -> 12
    | Some cursor, _ -> cursor
    | None, None -> 0
  in
  let signature =
    match ctx.signature with
    | Some none ->
        let signature =
          Option.fold ~none ~some:(fun hdr -> digest hdr none) hdr
        in
        Some signature
    | none -> none
  in
  let ctx = { ctx with signature } in
  match hdr with
  | Some hdr ->
      let next () = consume ctx targets cursor in
      fun () -> Seq.Cons (hdr, next)
  | None -> fun () -> consume ctx targets cursor

let delta_from_pack ~ref_length ~windows =
  let load uid (t, _) =
    let size = Carton.size_of_uid t ~uid Carton.Size.zero in
    let blob = Carton.Blob.make ~size in
    Carton.of_uid t blob ~uid
  in
  let stored = Hashtbl.create 0x7ff in
  Seq.map @@ fun entry ->
  Log.debug (fun m ->
      m "delta-ify %a" Carton.Uid.pp (Cartonnage.Entry.uid entry));
  let delta = Cartonnage.Entry.meta entry in
  let entry =
    match delta with
    | _, Some { source; raw; _ } when Hashtbl.mem stored source -> begin
        match Hashtbl.find_opt stored source with
        | Some depth ->
            let raw = (raw :> Cachet.bigstring) in
            let patch = Cartonnage.Patch.of_copy ~depth ~source raw in
            Cartonnage.Target.make ~patch entry
        | None -> Cartonnage.Target.make entry
      end
    | _ -> Cartonnage.Target.make entry
  in
  let k = Carton.Kind.to_int (Cartonnage.Target.kind entry) in
  let target = apply ~ref_length ~load ~window:windows.(k) entry in
  Hashtbl.add stored
    (Cartonnage.Target.uid entry)
    (Cartonnage.Target.depth entry);
  begin
    match (target, not (Window.is_full windows.(k))) with
    | None, false -> ()
    | None, true ->
        if Cartonnage.Target.depth entry < 50 then begin
          let uid = Cartonnage.Target.uid entry
          and meta = Cartonnage.Target.meta entry in
          let target = load uid meta in
          let source = Cartonnage.Target.to_source entry ~target in
          Log.debug (fun m ->
              m "add %a as a possible source (depth: %d)" Carton.Uid.pp
                (Cartonnage.Source.uid source)
                (Cartonnage.Target.depth entry));
          append ~window:windows.(k) source
        end
    | Some target, _ ->
        if Cartonnage.Target.depth entry < 50 then begin
          let source = Cartonnage.Target.to_source entry ~target in
          Log.debug (fun m ->
              m "add %a as a possible source (depth: %d)" Carton.Uid.pp
                (Cartonnage.Source.uid source)
                (Cartonnage.Target.depth entry));
          append ~window:windows.(k) source
        end
  end;
  entry

type sort = {
    sort:
      'a. 'a Cartonnage.Entry.t array list -> int * 'a Cartonnage.Entry.t Seq.t
}
[@@unboxed]

let merge :
       cfg:config
    -> digest:Carton.First_pass.digest
    -> sort:sort
    -> ?level:int
    -> Fpath.t list
    -> string Seq.t =
 fun ~cfg ~digest ~sort ?level filenames ->
  let ref_length = cfg.ref_length in
  let fn filename = entries_of_pack ~cfg ~digest filename in
  let entries = Miou.parallel fn filenames in
  let entries =
    List.map (function Ok arr -> arr | Error exn -> raise exn) entries
  in
  let with_header, entries = sort.sort entries in
  let windows = Array.init 4 (fun _ -> Window.make ()) in
  Log.debug (fun m -> m "Start to delta-ify entries");
  let targets = delta_from_pack ~ref_length ~windows entries in
  let load uid (t, _) =
    let size = Carton.size_of_uid t ~uid Carton.Size.zero in
    let blob = Carton.Blob.make ~size in
    Carton.of_uid t blob ~uid
  in
  to_pack ~with_header ~with_signature:digest ?level ~load targets
