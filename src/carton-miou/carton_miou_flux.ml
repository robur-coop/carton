let src = Logs.Src.create "carton-miou.flux"

module Log = (val Logs.src_log src : Logs.LOG)

type entry =
  [ `Number of int
  | `Inflate of (Carton.Kind.t * int) option * string
  | `Entry of Carton.First_pass.entry
  | `Hash of string ]

type 'acc _first_pass = {
    decoder: Carton.First_pass.decoder
  ; input: Bstr.t
  ; first: bool
  ; str: string
  ; src_off: int
  ; src_len: int
  ; acc: 'acc
}

let first_pass ~digest ~ref_length =
  let flow (Flux.Sink k) =
    let rec until_await_or_peek t =
      assert (k.full t.acc = false);
      match Carton.First_pass.decode t.decoder with
      | `Inflate (payload, decoder) ->
          let kind = Carton.First_pass.kind decoder in
          let acc = k.push t.acc (`Inflate (kind, payload)) in
          let t = { t with decoder; acc } in
          if k.full acc then t else until_await_or_peek t
      | `Entry (entry, decoder) when t.first ->
          let n = Carton.First_pass.number_of_objects decoder in
          let acc = t.acc in
          let acc = k.push acc (`Number n) in
          let t = { t with first= false; decoder; acc } in
          if k.full acc then t
          else
            let acc = k.push acc (`Entry entry) in
            let t = { t with acc } in
            if k.full acc then t else until_await_or_peek t
      | `Entry (entry, decoder) ->
          let acc = k.push t.acc (`Entry entry) in
          let t = { t with decoder; acc } in
          if k.full acc then t else until_await_or_peek t
      | `Malformed err -> failwith err
      | `End hash ->
          let acc = k.push t.acc (`Hash hash) in
          { t with acc }
      | `Await decoder ->
          if t.src_len = 0 then { t with decoder }
          else begin
            let len = Int.min (Bstr.length t.input) t.src_len in
            let src_off = t.src_off in
            Bstr.blit_from_string t.str ~src_off t.input ~dst_off:0 ~len;
            let decoder = Carton.First_pass.src decoder t.input 0 len in
            let src_off = src_off + len and src_len = t.src_len - len in
            let t = { t with decoder; src_off; src_len } in
            until_await_or_peek t
          end
      | `Peek decoder ->
          let dst_off = Carton.First_pass.src_rem decoder in
          if t.src_len = 0 then { t with decoder }
          else begin
            let len = Int.min (Bstr.length t.input - dst_off) t.src_len in
            let src_off = t.src_off in
            Bstr.blit_from_string t.str ~src_off t.input ~dst_off ~len;
            let decoder =
              Carton.First_pass.src decoder t.input 0 (dst_off + len)
            in
            let src_off = src_off + len and src_len = t.src_len - len in
            let t = { t with decoder; src_off; src_len } in
            until_await_or_peek t
          end
    in
    let init () =
      let window = De.make_window ~bits:15 in
      let allocate _bits = window in
      let output = Bstr.create De.io_buffer_size in
      let input = Bstr.create De.io_buffer_size in
      let decoder =
        Carton.First_pass.decoder ~output ~allocate ~ref_length ~digest `Manual
      in
      let acc = k.init ()
      and str = String.empty
      and src_off = 0
      and src_len = 0
      and first = true in
      { decoder; input; acc; first; str; src_off; src_len }
    in
    let push t str =
      if not (k.full t.acc) then
        let src_off = 0 and src_len = String.length str in
        let t = { t with str; src_off; src_len } in
        until_await_or_peek t
      else t
    in
    let full t = k.full t.acc in
    let stop t = k.stop t.acc in
    Flux.Sink { init; push; full; stop }
  in
  { Flux.flow }

type offset = int
type uid = Carton.Uid.t
type size = Carton.Size.t
type crc = Optint.t

type _oracle =
  | Oracle : {
        children_by_offset: (offset, offset list) Hashtbl.t
      ; children_by_uid: (uid, offset list) Hashtbl.t
      ; sizes: (offset, size ref) Hashtbl.t
      ; where: (offset, int) Hashtbl.t
      ; cursors: (int, offset) Hashtbl.t
      ; crcs: (offset, crc) Hashtbl.t
      ; bases: (int, offset) Hashtbl.t
      ; index: (uid, offset) Hashtbl.t
      ; ref_index: (offset, uid) Hashtbl.t
      ; mutable number_of_objects: int
      ; mutable hash: string
      ; mutable ctx: 'ctx option
      ; identify: 'ctx Carton.First_pass.identify
    }
      -> _oracle

let oracle ~identify =
  let update_size (Oracle t) ~parent offset (size : Carton.Size.t) =
    let cell : Carton.Size.t ref = Hashtbl.find t.sizes parent in
    (cell := Carton.Size.(max !cell size));
    Hashtbl.replace t.sizes offset cell
  in
  let new_child (Oracle t) ~parent child =
    match parent with
    | `Ofs parent -> begin
        match Hashtbl.find_opt t.children_by_offset parent with
        | None -> Hashtbl.add t.children_by_offset parent [ child ]
        | Some offsets ->
            Hashtbl.replace t.children_by_offset parent (child :: offsets)
      end
    | `Ref parent -> begin
        match Hashtbl.find_opt t.children_by_uid parent with
        | None -> Hashtbl.add t.children_by_uid parent [ child ]
        | Some offsets ->
            Hashtbl.replace t.children_by_uid parent (child :: offsets)
      end
  in
  let init () =
    let children_by_offset = Hashtbl.create 0x7ff in
    let children_by_uid = Hashtbl.create 0x7ff in
    let sizes = Hashtbl.create 0x7ff in
    let where = Hashtbl.create 0x7ff in
    let cursors = Hashtbl.create 0x7ff in
    let crcs = Hashtbl.create 0x7ff in
    let bases = Hashtbl.create 0x7ff in
    let index = Hashtbl.create 0x7ff in
    let ref_index = Hashtbl.create 0x7ff in
    let number_of_objects = 0 in
    let hash = String.empty in
    let ctx = None in
    Oracle
      {
        children_by_offset
      ; children_by_uid
      ; sizes
      ; where
      ; cursors
      ; crcs
      ; bases
      ; index
      ; ref_index
      ; number_of_objects
      ; hash
      ; ctx
      ; identify
      }
  in
  let push (Oracle t as oracle) entry =
    let () =
      match entry with
      | `Number n -> t.number_of_objects <- n
      | `Hash value -> t.hash <- value
      | `Inflate (None, _) -> ()
      | `Inflate (Some (k, size), str) -> begin
          let open Carton in
          let open First_pass in
          match t.ctx with
          | None ->
              let ctx0 = t.identify.init k (Carton.Size.of_int_exn size) in
              let ctx0 = t.identify.feed (Bstr.of_string str) ctx0 in
              t.ctx <- Some ctx0
          | Some ctx0 ->
              let ctx0 = t.identify.feed (Bstr.of_string str) ctx0 in
              t.ctx <- Some ctx0
        end
      | `Entry entry -> begin
          let offset = entry.Carton.First_pass.offset in
          let size = entry.Carton.First_pass.size in
          let crc = entry.Carton.First_pass.crc in
          Hashtbl.add t.where offset entry.number;
          Hashtbl.add t.cursors entry.number offset;
          Hashtbl.add t.crcs offset crc;
          match entry.Carton.First_pass.kind with
          | Carton.First_pass.Base kind ->
              Hashtbl.add t.sizes offset (ref size);
              Hashtbl.add t.bases entry.number offset;
              let uid =
                match Option.map t.identify.serialize t.ctx with
                | Some uid -> uid
                | None ->
                    let size = entry.Carton.First_pass.size in
                    let ctx = t.identify.init kind size in
                    t.identify.serialize ctx
              in
              t.ctx <- None;
              Hashtbl.add t.index uid offset
          | Ofs { sub; source; target; _ } ->
              let abs_parent = offset - sub in
              update_size oracle ~parent:abs_parent offset
                (Carton.Size.max target source);
              new_child oracle ~parent:(`Ofs abs_parent) offset
          | Ref { ptr; source; target; _ } ->
              let () =
                match Hashtbl.find_opt t.index ptr with
                | Some parent ->
                    update_size oracle ~parent offset
                      (Carton.Size.max source target)
                | None ->
                    Hashtbl.add t.sizes offset
                      (ref (Carton.Size.max source target))
              in
              Hashtbl.add t.ref_index offset ptr;
              new_child oracle ~parent:(`Ref ptr) offset
        end
    in
    oracle
  in
  let full = Fun.const false in
  let stop (Oracle t as oracle) =
    Hashtbl.iter
      (fun offset ptr ->
        match Hashtbl.find_opt t.index ptr with
        | Some parent ->
            update_size oracle ~parent offset !(Hashtbl.find t.sizes offset)
        | None -> ())
      t.ref_index;
    let children ~cursor ~uid =
      match
        ( Hashtbl.find_opt t.children_by_offset cursor
        , Hashtbl.find_opt t.children_by_uid uid )
      with
      | Some (_ :: _ as children), (Some [] | None) -> children
      | (Some [] | None), Some (_ :: _ as children) -> children
      | (None | Some []), (None | Some []) -> []
      | Some lst0, Some lst1 ->
          List.(sort_uniq Int.compare (rev_append lst0 lst1))
    in
    let where ~cursor = Hashtbl.find t.where cursor in
    let size ~cursor = !(Hashtbl.find t.sizes cursor) in
    let checksum ~cursor = Hashtbl.find t.crcs cursor in
    let is_base ~pos = Hashtbl.find_opt t.bases pos in
    let cursor ~pos = Hashtbl.find t.cursors pos in
    {
      Carton.identify= Carton.Identify identify
    ; children
    ; where
    ; cursor
    ; size
    ; checksum
    ; is_base
    ; number_of_objects= t.number_of_objects
    ; hash= t.hash
    }
  in
  Flux.Sink { init; push; full; stop }

type base = { value: Carton.Value.t; uid: Carton.Uid.t; depth: int }

let identify (Carton.Identify gen) ~kind ~len bstr =
  let ctx = gen.Carton.First_pass.init kind (Carton.Size.of_int_exn len) in
  let ctx = gen.Carton.First_pass.feed (Bigarray.Array1.sub bstr 0 len) ctx in
  gen.Carton.First_pass.serialize ctx

let rec resolve_tree q t oracle matrix ~base = function
  | [||] -> ()
  | [| cursor |] ->
      let value = Carton.of_offset_with_source t base.value ~cursor in
      let len = Carton.Value.length value
      and bstr = Carton.Value.bigstring value
      and kind = Carton.Value.kind value in
      let uid = identify oracle.Carton.identify ~kind ~len bstr
      and pos = oracle.where ~cursor
      and crc = oracle.checksum ~cursor
      and depth = succ base.depth in
      (* NOTE(dinosaure): see our comment on [verify] about copy. *)
      let copy = Bstr.(copy (sub bstr ~off:0 ~len)) in
      Flux.Bqueue.put q (Carton.Value.make ~kind ~depth copy, cursor, uid);
      matrix.(pos) <-
        Carton.Resolved_node { cursor; uid; crc; kind; depth; parent= base.uid };
      let children = oracle.children ~cursor ~uid in
      let children = Array.of_list children in
      let value = Carton.Value.flip value in
      let base = { value; uid; depth } in
      resolve_tree q t oracle matrix ~base children
  | cursors ->
      let source = Carton.Value.source base.value in
      let source = Bstr.copy source in
      let rec go idx =
        if idx < Array.length cursors then begin
          let cursor = cursors.(idx) in
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
          (* NOTE(dinosaure): see our comment on [verify] about copy. *)
          let copy = Bstr.(copy (sub bstr ~off:0 ~len)) in
          Flux.Bqueue.put q (Carton.Value.make ~kind ~depth copy, cursor, uid);
          matrix.(pos) <-
            Resolved_node { cursor; uid; crc; kind; depth; parent= base.uid };
          let children = oracle.children ~cursor ~uid in
          let children = Array.of_list children in
          let value = Carton.Value.with_source ~source:dirty value in
          let value = Carton.Value.flip value in
          let base = { value; uid; depth } in
          resolve_tree q t oracle matrix ~base children;
          go (succ idx)
        end
      in
      go 0

let is_unresolved_base = function
  | Carton.Unresolved_base _ -> true
  | _ -> false

let verify ?(threads = 4) q t oracle matrix =
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
    Log.debug (fun m -> m "Resolve object %d/%d" pos (Array.length matrix));
    if pos < Array.length matrix then begin
      let[@warning "-8"] (Carton.Unresolved_base { cursor }) = matrix.(pos) in
      let size = oracle.Carton.size ~cursor in
      let blob = Carton.Blob.make ~size in
      let value = Carton.of_offset t blob ~cursor in
      let len = Carton.Value.length value
      and bstr = Carton.Value.bigstring value
      and kind = Carton.Value.kind value in
      let uid = identify oracle.Carton.identify ~kind ~len bstr
      and crc = oracle.checksum ~cursor in
      (* NOTE(dinosaure): due to the queue, we must copy our internal bigarray
         to safely pass it to another process which will own our copy. We can
         safely re-use our [blob] then. *)
      let copy = Bstr.(copy (sub bstr ~off:0 ~len)) in
      Flux.Bqueue.put q (Carton.Value.make ~kind copy, cursor, uid);
      matrix.(pos) <- Resolved_base { cursor; uid; crc; kind };
      let children = oracle.children ~cursor ~uid in
      let children = Array.of_list children in
      let base = Carton.{ value= Value.flip value; uid; depth= 1 } in
      resolve_tree q t oracle matrix ~base children;
      fn t
    end
  in
  let init _thread = Carton.copy t in
  let results =
    if threads > 0 then Miou.parallel fn (List.init threads init)
    else try fn t; [ Ok () ] with exn -> [ Error exn ]
  in
  List.iter (function Ok () -> () | Error exn -> raise exn) results

let entries ?threads pack oracle =
  Flux.Source.with_task ~size:0x7ff @@ fun q ->
  let matrix =
    Array.init oracle.Carton.number_of_objects @@ fun pos ->
    match oracle.is_base ~pos with
    | Some cursor -> Carton.Unresolved_base { cursor }
    | None -> Unresolved_node { cursor= oracle.cursor ~pos }
  in
  verify ?threads q pack oracle matrix;
  Flux.Bqueue.close q
