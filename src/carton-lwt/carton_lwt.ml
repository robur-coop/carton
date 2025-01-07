open Lwt

let failwithf fmt = Format.kasprintf failwith fmt
let src = Logs.Src.create "carton-lwt"

external bigstring_set_uint8 : De.bigstring -> int -> int -> unit
  = "%caml_ba_set_1"

external bigstring_set_int32_ne : De.bigstring -> int -> int32 -> unit
  = "%caml_bigstring_set32"

let bigstring_blit_from_bytes src ~src_off dst ~dst_off ~len =
  let len0 = len land 3 in
  let len1 = len lsr 2 in
  for i = 0 to len1 - 1 do
    let i = i * 4 in
    let v = Bytes.get_int32_ne src (src_off + i) in
    bigstring_set_int32_ne dst (dst_off + i) v
  done;
  for i = 0 to len0 - 1 do
    let i = (len1 * 4) + i in
    let v = Bytes.get_uint8 src (src_off + i) in
    bigstring_set_uint8 dst (dst_off + i) v
  done

let bigstring_blit_from_string src ~src_off dst ~dst_off ~len =
  bigstring_blit_from_bytes
    (Bytes.unsafe_of_string src)
    ~src_off dst ~dst_off ~len

module Log = (val Logs.src_log src : Logs.LOG)

let ignore3 _ _ = Lwt.return_unit
let ignorem ~max:_ _ = Lwt.return_unit

let header_of_entry t ~cursor =
  Cachet_lwt.get_string (Carton.cache t) ~len:10 cursor >|= fun str ->
  let p = ref 0 in
  let c = ref (String.get_uint8 str 0) in
  incr p;
  let kind = (!c lsr 4) land 7 in
  let size = ref (!c land 15) in
  let shft = ref 4 in
  while !c land 0x80 != 0 do
    c := String.get_uint8 str !p;
    incr p;
    size := !size + ((!c land 0x7f) lsl !shft);
    shft := !shft + 7
  done;
  ((kind, !size), cursor + !p)

let header_of_ofs_delta t ~cursor =
  Log.debug (fun m -> m "decode ofs header at %08x" cursor);
  Cachet_lwt.get_string (Carton.cache t) ~len:10 cursor >|= fun str ->
  let p = ref 0 in
  let c = ref (String.get_uint8 str !p) in
  incr p;
  let rel_offset = ref (!c land 127) in
  while !c land 128 != 0 do
    incr rel_offset;
    c := String.get_uint8 str !p;
    incr p;
    rel_offset := (!rel_offset lsl 7) + (!c land 127)
  done;
  (!rel_offset, cursor + !p)

let uncompress t kind blob ~cursor =
  let anchor = cursor in
  let o = Carton.Blob.payload blob in
  let rec go ~real_length ~flushed slice decoder =
    match Zl.Inf.decode decoder with
    | `Malformed err -> failwithf "object <%08x>: %s" anchor err
    | `End decoder ->
        let len = Bigarray.Array1.dim o - Zl.Inf.dst_rem decoder in
        assert (flushed || ((not flushed) && len = 0));
        (* XXX(dinosaure): we gave a [o] buffer which is enough to store
           inflated data. At the end, [decoder] should not return more than one
           [`Flush]. A special case is when we inflate nothing: [`Flush] never
           appears and we reach [`End] directly, so [!p (still) = false and len
           (must) = 0]. *)
        let value = Carton.Value.of_blob ~kind ~length:real_length blob in
        Lwt.return value
    | `Flush decoder ->
        let real_length = Bigarray.Array1.dim o - Zl.Inf.dst_rem decoder in
        assert (not flushed);
        let decoder = Zl.Inf.flush decoder in
        (go [@tailcall]) ~real_length ~flushed:true slice decoder
    | `Await decoder -> begin
        Cachet_lwt.next (Carton.cache t) slice >>= function
        | Some ({ payload; length; _ } as slice) ->
            let decoder =
              Zl.Inf.src decoder (payload :> De.bigstring) 0 length
            in
            (go [@tailcall]) ~real_length ~flushed slice decoder
        | None ->
            let decoder = Zl.Inf.src decoder De.bigstring_empty 0 0 in
            (go [@tailcall]) ~real_length ~flushed slice decoder
      end
  in
  Log.debug (fun m -> m "load %08x" cursor);
  Cachet_lwt.load (Carton.cache t) cursor >>= function
  | Some ({ offset; payload; length } as slice) ->
      let off = cursor - offset in
      let len = length - off in
      let allocate = Carton.allocate t in
      let decoder = Zl.Inf.decoder `Manual ~o ~allocate in
      let decoder = Zl.Inf.src decoder (payload :> De.bigstring) off len in
      go ~real_length:0 ~flushed:false slice decoder
  | None -> assert false

let of_delta t kind blob ~depth ~cursor =
  let tmp = Carton.tmp t in
  let allocate = Carton.allocate t in
  let decoder = Carton.Zh.M.decoder ~o:tmp ~allocate `Manual in
  let rec go slice blob decoder =
    match Carton.Zh.M.decode decoder with
    | `End decoder ->
        let length = Carton.Zh.M.dst_len decoder in
        let value = Carton.Value.of_blob ~kind ~length ~depth blob in
        Lwt.return value
    | `Malformed err -> failwith err
    | `Header (src_len, dst_len, decoder) ->
        let source = Carton.Blob.source blob in
        let payload = Carton.Blob.payload blob in
        Log.debug (fun m ->
            m "specify the source to apply the patch on %08x" cursor);
        Log.debug (fun m ->
            m "src_len: %d, required: %d" (Bigarray.Array1.dim source) src_len);
        let decoder = Carton.Zh.M.source decoder source in
        Log.debug (fun m -> m "dst_len: %d" dst_len);
        let decoder = Carton.Zh.M.dst decoder payload 0 dst_len in
        (go [@tailcall]) slice blob decoder
    | `Await decoder -> begin
        Cachet_lwt.next (Carton.cache t) slice >>= function
        | None ->
            let decoder = Carton.Zh.M.src decoder De.bigstring_empty 0 0 in
            (go [@tailcall]) slice blob decoder
        | Some ({ payload; length; _ } as slice) ->
            let decoder =
              Carton.Zh.M.src decoder (payload :> De.bigstring) 0 length
            in
            (go [@tailcall]) slice blob decoder
      end
  in
  Log.debug (fun m -> m "load %08x" cursor);
  Cachet_lwt.load (Carton.cache t) cursor >>= function
  | Some ({ offset; payload; length } as slice) ->
      let off = cursor - offset in
      let len = length - off in
      let decoder = Zh.M.src decoder (payload :> De.bigstring) off len in
      go slice blob decoder
  | None -> assert false

let of_offset_with_source t kind blob ~depth ~cursor =
  header_of_entry t ~cursor >>= fun ((kind', _size), cursor') ->
  match kind' with
  | 0b000 | 0b101 -> raise Carton.Bad_type
  | 0b001 ->
      assert (kind = `A);
      uncompress t `A blob ~cursor:cursor'
  | 0b010 ->
      assert (kind = `B);
      uncompress t `B blob ~cursor:cursor'
  | 0b011 ->
      assert (kind = `C);
      uncompress t `C blob ~cursor:cursor'
  | 0b100 ->
      assert (kind = `D);
      uncompress t `D blob ~cursor:cursor'
  | 0b110 ->
      header_of_ofs_delta t ~cursor:cursor' >>= fun (_rel_offset, cursor'') ->
      of_delta t kind blob ~depth ~cursor:cursor''
  | 0b111 -> of_delta t kind blob ~depth ~cursor:(cursor' + Carton.ref_length t)
  | _ -> assert false

let of_offset_with_source t value ~cursor =
  let kind = Carton.Value.kind value
  and blob = Carton.Value.blob value
  and depth = Carton.Value.depth value in
  of_offset_with_source t kind blob ~depth ~cursor

let bigstring_copy bstr =
  let len = Bigarray.Array1.dim bstr in
  let bstr' = Bigarray.Array1.create Bigarray.char Bigarray.c_layout len in
  Cachet.memcpy bstr ~src_off:0 bstr' ~dst_off:0 ~len;
  bstr'

type base = { value: Carton.Value.t; uid: Carton.Uid.t; depth: int }

let identify (Carton.Identify gen) ~kind ~len bstr =
  let ctx = gen.Carton.First_pass.init kind (Carton.Size.of_int_exn len) in
  let ctx = gen.Carton.First_pass.feed (Bigarray.Array1.sub bstr 0 len) ctx in
  gen.Carton.First_pass.serialize ctx

let rec resolve_tree ?(on = ignore3) t oracle matrix ~(base : base) = function
  | [||] -> Lwt.return_unit
  | [| cursor |] ->
      Log.debug (fun m -> m "resolve node at %08x" cursor);
      of_offset_with_source t base.value ~cursor >>= fun value ->
      let len = Carton.Value.length value
      and bstr = Carton.Value.bigstring value
      and kind = Carton.Value.kind value in
      let uid = identify oracle.Carton.identify ~kind ~len bstr
      and pos = oracle.where ~cursor
      and crc = oracle.checksum ~cursor
      and depth = succ base.depth in
      on value uid >>= fun () ->
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
          of_offset_with_source t src ~cursor >>= fun value ->
          let len = Carton.Value.length value
          and bstr = Carton.Value.bigstring value
          and kind = Carton.Value.kind value in
          let uid = identify oracle.Carton.identify ~kind ~len bstr
          and pos = oracle.where ~cursor
          and crc = oracle.checksum ~cursor
          and depth = succ base.depth in
          on value uid >>= fun () ->
          matrix.(pos) <-
            Resolved_node { cursor; uid; crc; kind; depth; parent= base.uid };
          let children = oracle.children ~cursor ~uid in
          Log.debug (fun m ->
              m "resolve children of %08x %a" cursor Carton.Uid.pp uid);
          let children = Array.of_list children in
          let value = Carton.Value.with_source ~source:dirty value in
          let value = Carton.Value.flip value in
          let base = { value; uid; depth } in
          resolve_tree ~on t oracle matrix ~base children >>= fun () ->
          go (succ idx)
        end
        else Lwt.return_unit
      in
      go 0

let is_unresolved_base = function
  | Carton.Unresolved_base _ -> true
  | _ -> false

let verify ?(threads = 4) ?(on = ignore3) t oracle matrix =
  let mutex = Lwt_mutex.create () in
  let idx = ref 0 in
  let rec fn t =
    Lwt_mutex.with_lock mutex
      begin
        fun () ->
          while
            !idx < Array.length matrix
            && is_unresolved_base matrix.(!idx) = false
          do
            incr idx
          done;
          let value = !idx in
          incr idx; Lwt.return value
      end
    >>= fun pos ->
    if pos < Array.length matrix then begin
      let[@warning "-8"] (Carton.Unresolved_base { cursor }) = matrix.(pos) in
      let size = oracle.Carton.size ~cursor in
      Log.debug (fun m -> m "resolve base at %08x" cursor);
      Log.debug (fun m -> m "allocate a blob of %d byte(s)" (size :> int));
      let blob = Carton.Blob.make ~size in
      let value = Carton.of_offset t blob ~cursor in
      let len = Carton.Value.length value
      and bstr = Carton.Value.bigstring value
      and kind = Carton.Value.kind value in
      let uid = identify oracle.Carton.identify ~kind ~len bstr
      and crc = oracle.checksum ~cursor in
      on value uid >>= fun () ->
      matrix.(pos) <- Resolved_base { cursor; uid; crc; kind };
      let children = oracle.children ~cursor ~uid in
      let children = Array.of_list children in
      let base = Carton.{ value= Value.flip value; uid; depth= 1 } in
      resolve_tree ~on t oracle matrix ~base children >>= fun () -> fn t
    end
    else Lwt.return_unit
  in
  let init _thread = Carton.copy t in
  Lwt_list.iter_p fn (List.init threads init) >>= fun _units -> Lwt.return_unit

type entry = { offset: int; crc: Optint.t; consumed: int; size: int }

type config = {
    threads: int option
  ; ref_length: int
  ; identify: Carton.identify
  ; on_entry: max:int -> entry -> unit Lwt.t
  ; on_object: Carton.Value.t -> Carton.Uid.t -> unit Lwt.t
}

let config ?(threads = 4) ?(on_entry = ignorem) ?(on_object = ignore3)
    ~ref_length identify =
  { threads= Some threads; ref_length; identify; on_entry; on_object }

let compile ?(on = ignorem) ~identify ~digest_length seq =
  let children_by_offset = Hashtbl.create 0x7ff in
  let children_by_uid = Hashtbl.create 0x7ff in
  let sizes : (int, Carton.Size.t ref) Hashtbl.t = Hashtbl.create 0x7ff in
  let where = Hashtbl.create 0x7ff in
  let crcs = Hashtbl.create 0x7ff in
  let is_base = Hashtbl.create 0x7ff in
  let index = Hashtbl.create 0x7ff in
  let ref_index = Hashtbl.create 0x7ff in
  let hash = ref (String.make digest_length '\000') in
  let update_size ~parent offset size =
    Log.debug (fun m ->
        m "Update the size for %08x (parent: %08x)" offset parent);
    let cell : Carton.Size.t ref = Hashtbl.find sizes parent in
    (cell := Carton.Size.(max !cell size));
    Hashtbl.add sizes offset cell
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
  let pos = ref 0 in
  let fn = function
    | `Number n ->
        number_of_objects := n;
        Lwt.return_unit
    | `Hash value ->
        hash := value;
        Lwt.return_unit
    | `Entry entry -> begin
        let offset = entry.Carton.First_pass.offset in
        let size = entry.Carton.First_pass.size in
        let consumed = entry.Carton.First_pass.consumed in
        let crc = entry.Carton.First_pass.crc in
        on ~max:!number_of_objects { offset; crc; consumed; size:> int }
        >|= fun () ->
        Hashtbl.add where offset !pos;
        Hashtbl.add crcs offset crc;
        match entry.Carton.First_pass.kind with
        | Carton.First_pass.Base (_, uid) ->
            Hashtbl.add sizes offset (ref size);
            Hashtbl.add is_base !pos offset;
            Hashtbl.add index uid offset;
            incr pos
        | Ofs { sub; source; target } ->
            Log.debug (fun m ->
                m "new OBJ_OFS object at %08x (rel: %08x)" offset sub);
            let parent = offset - sub in
            update_size ~parent offset (Carton.Size.max source target);
            Log.debug (fun m -> m "new child for %08x at %08x" parent offset);
            new_child ~parent:(`Ofs parent) offset;
            incr pos
        | Ref { ptr; source; target; _ } ->
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
  Lwt_seq.iter_p fn seq >|= fun () ->
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
  ; number_of_objects= !number_of_objects
  ; hash= !hash
  }

type ctx = {
    output: De.bigstring
  ; allocate: int -> De.window
  ; ref_length: int
  ; digest: Carton.First_pass.digest
  ; identify: Carton.identify
}

let of_stream_to_store ctx ~append stream =
  let input = De.bigstring_create 0x7ff in
  let first = ref true in
  let rec go decoder (str, src_off, src_len) () =
    match Carton.First_pass.decode decoder with
    | `Await decoder ->
        if src_len == 0 then
          Lwt_stream.get stream >>= function
          | Some str ->
              let len =
                Int.min (Bigarray.Array1.dim input) (String.length str)
              in
              bigstring_blit_from_string str ~src_off:0 input ~dst_off:0 ~len;
              append str ~off:0 ~len >>= fun () ->
              let decoder = Carton.First_pass.src decoder input 0 len in
              go decoder (str, len, String.length str - len) ()
          | None ->
              let decoder =
                Carton.First_pass.src decoder De.bigstring_empty 0 0
              in
              go decoder (String.empty, 0, 0) ()
        else begin
          let len = Int.min (Bigarray.Array1.dim input) src_len in
          bigstring_blit_from_string str ~src_off input ~dst_off:0 ~len;
          append str ~off:src_off ~len >>= fun () ->
          let decoder = Carton.First_pass.src decoder input 0 len in
          go decoder (str, src_off + len, src_len - len) ()
        end
    | `Peek decoder ->
        let dst_off = Carton.First_pass.src_rem decoder in
        if src_len == 0 then
          Lwt_stream.get stream >>= function
          | Some str ->
              let len =
                Int.min
                  (Bigarray.Array1.dim input - dst_off)
                  (String.length str)
              in
              bigstring_blit_from_string str ~src_off:0 input ~dst_off ~len;
              append str ~off:0 ~len >>= fun () ->
              let decoder =
                Carton.First_pass.src decoder input 0 (dst_off + len)
              in
              go decoder (str, len, String.length str - len) ()
          | None ->
              let decoder =
                Carton.First_pass.src decoder De.bigstring_empty 0 0
              in
              go decoder (String.empty, 0, 0) ()
        else begin
          let len = Int.min (Bigarray.Array1.dim input - dst_off) src_len in
          bigstring_blit_from_string str ~src_off input ~dst_off ~len;
          append str ~off:src_off ~len >>= fun () ->
          let decoder = Carton.First_pass.src decoder input 0 (dst_off + len) in
          go decoder (str, src_off + len, src_len - len) ()
        end
    | `Entry (entry, decoder) ->
        let next = go decoder (str, src_off, src_len) in
        begin
          match !first with
          | true ->
              first := false;
              let n = Carton.First_pass.number_of_objects decoder in
              let next = Lwt_seq.cons (`Entry entry) next in
              Lwt.return (Lwt_seq.Cons (`Number n, next))
          | false -> Lwt.return (Lwt_seq.Cons (`Entry entry, next))
        end
    | `Malformed err -> failwith err
    | `End hash -> Lwt.return (Lwt_seq.Cons (`Hash hash, Lwt_seq.empty))
  in
  let decoder =
    let {
      output
    ; allocate
    ; ref_length
    ; digest
    ; identify= Carton.Identify identify
    } =
      ctx
    in
    Carton.First_pass.decoder ~output ~allocate ~ref_length ~digest ~identify
      `Manual
  in
  go decoder (String.empty, 0, 0)

let never uid = failwithf "Impossible to find the object %a" Carton.Uid.pp uid

let make ?z ~ref_length ?(index = never) cache =
  let z = match z with None -> De.bigstring_create 0x7ff | Some z -> z in
  let allocate bits = De.make_window ~bits in
  Carton.of_cache cache ~z ~allocate ~ref_length index

let verify_from_stream
    ~cfg:{ threads; ref_length; identify; on_entry; on_object } ~digest ~append
    cache stream =
  let z = De.bigstring_create De.io_buffer_size in
  let seq =
    let allocate bits = De.make_window ~bits in
    let ctx = { output= z; allocate; ref_length; digest; identify } in
    of_stream_to_store ctx ~append stream
  in
  let (Carton.First_pass.Digest ({ length= digest_length; _ }, _)) = digest in
  compile ~on:on_entry ~identify ~digest_length seq >>= fun oracle ->
  let t = make ~z ~ref_length cache in
  let matrix =
    Array.init oracle.Carton.number_of_objects @@ fun pos ->
    match oracle.is_base ~pos with
    | Some cursor -> Carton.Unresolved_base { cursor }
    | None -> Unresolved_node
  in
  verify ?threads ~on:on_object t oracle matrix >|= fun () ->
  (matrix, oracle.hash)

let _max_depth = 50

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

module Window = struct
  type 'meta t = {
      arr: 'meta Cartonnage.Source.t array
    ; mutable rd_pos: int
    ; mutable wr_pos: int
  }

  let make () = { arr= Array.make 0x100 (Obj.magic ()); rd_pos= 0; wr_pos= 0 }
  let is_full { rd_pos; wr_pos; arr } = wr_pos - rd_pos = Array.length arr
end

let apply ~ref_length ~load ~window:t entry =
  let len = t.Window.wr_pos - t.Window.rd_pos in
  let msk = Array.length t.Window.arr - 1 in
  let uid = Cartonnage.Target.uid entry
  and meta = Cartonnage.Target.meta entry in
  let target = Lazy.from_fun (fun () -> load uid meta) in
  let rec go i =
    if i < len then begin
      let source = t.Window.arr.((t.Window.rd_pos + i) land msk) in
      if
        Cartonnage.Source.depth source <= 50
        && should_we_apply ~ref_length ~source entry
      then begin
        Lazy.force target >>= fun target ->
        Cartonnage.Target.diff entry ~source ~target;
        go (succ i)
      end
      else go (succ i)
    end
    else Lwt.return_unit
  in
  go 0 >>= fun () ->
  if Lazy.is_val target then Lazy.force target >|= Option.some
  else Lwt.return_none

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
  Lwt_seq.map_s @@ fun entry ->
  let entry = Cartonnage.Target.make entry in
  let k = Carton.Kind.to_int (Cartonnage.Target.kind entry) in
  apply ~ref_length ~load ~window:windows.(k) entry >>= fun target ->
  Log.debug (fun m ->
      m "patch found for %a? %b" Carton.Uid.pp
        (Cartonnage.Target.uid entry)
        Option.(is_some (Cartonnage.Target.patch entry)));
  begin
    match (target, not (Window.is_full windows.(k))) with
    | None, false -> Lwt.return_unit
    | None, true ->
        let uid = Cartonnage.Target.uid entry
        and meta = Cartonnage.Target.meta entry in
        load uid meta >>= fun target ->
        let source = Cartonnage.Target.to_source entry ~target in
        append ~window:windows.(k) source;
        Lwt.return_unit
    | Some target, _ ->
        let source = Cartonnage.Target.to_source entry ~target in
        append ~window:windows.(k) source;
        Lwt.return_unit
  end
  >|= fun () -> entry

type cartonnage = {
    where: (Carton.Uid.t, int) Hashtbl.t
  ; buffers: Cartonnage.buffers
  ; signature: Carton.First_pass.digest option
}

let digest str (Carton.First_pass.Digest (({ feed_bytes; _ } as hash), ctx)) =
  let len = String.length str in
  let ctx = feed_bytes (Bytes.unsafe_of_string str) ~off:0 ~len ctx in
  Carton.First_pass.Digest (hash, ctx)

let to_pack ?with_header ?with_signature ?cursor ?level ~load targets () =
  let ctx =
    let o = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000
    and i = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000
    and q = De.Queue.create 0x1000
    and w = De.Lz77.make_window ~bits:15 in
    let buffers = Cartonnage.{ o; i; q; w } in
    let where = Hashtbl.create 0x100 in
    { where; buffers; signature= with_signature }
  in
  let rec go ctx encoder cursor : _ Lwt_seq.t =
   fun () ->
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
        let next = go ctx encoder (cursor + len) in
        Lwt.return (Lwt_seq.Cons (str, next))
    | `End -> consume ctx cursor ()
  and consume ctx cursor : _ Lwt_seq.t =
   fun () ->
    Lwt_stream.get targets >>= function
    | None -> begin
        match ctx.signature with
        | Some Carton.First_pass.(Digest ({ serialize; _ }, ctx)) ->
            let signature = serialize ctx in
            Lwt.return (Lwt_seq.Cons (signature, Lwt_seq.empty))
        | None -> Lwt.return Lwt_seq.Nil
      end
    | Some entry ->
        let uid = Cartonnage.Target.uid entry
        and meta = Cartonnage.Target.meta entry in
        load uid meta >>= fun target ->
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
        go ctx encoder cursor ()
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
      let next = consume ctx cursor in
      Lwt.return (Lwt_seq.Cons (hdr, next))
  | None -> consume ctx cursor ()

let index ~length ~hash_length ~ref_length cachet =
  Classeur.of_cachet ~length ~hash_length ~ref_length cachet
