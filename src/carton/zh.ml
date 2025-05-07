module N : sig
  type encoder
  type dst = [ `Buffer of Buffer.t | `Manual ]
  type ret = [ `Flush of encoder | `End ]

  val dst_rem : encoder -> int
  val dst : encoder -> Bstr.t -> int -> int -> encoder
  val encode : encoder -> ret

  val encoder :
       ?level:int
    -> i:Bstr.t
    -> q:De.Queue.t
    -> w:De.Lz77.window
    -> source:int
    -> Bstr.t
    -> dst
    -> Duff.hunk list
    -> encoder
end = struct
  type dst = [ `Buffer of Buffer.t | `Manual ]

  type encoder = {
      dst: dst
    ; o: Bstr.t
    ; o_pos: int
    ; o_max: int
    ; h: H.N.encoder
    ; z: Zl.Def.encoder
    ; t: Bstr.t
    ; d: [ `Copy of int * int | `Insert of string | `End | `Await ] list
  }

  type ret = [ `Flush of encoder | `End ]

  let flush k e =
    match e.dst with
    | `Manual -> `Flush e
    | `Buffer b ->
        for i = 0 to e.o_pos - 1 do
          Buffer.add_char b (Bstr.get e.o i)
        done;
        k { e with o_pos= 0 }

  let rec encode_z e =
    match Zl.Def.encode e.z with
    | `End z ->
        let len = Bstr.length e.o - Zl.Def.dst_rem z in
        let z = Zl.Def.dst z Bstr.empty 0 0 in
        if len > 0 then flush encode_z { e with z; o_pos= len } else `End
    | `Flush z ->
        let len = Bstr.length e.o - Zl.Def.dst_rem z in
        flush encode_z { e with z; o_pos= len }
    | `Await z -> (
        match e.d with
        | [] ->
            let z = Zl.Def.src z Bstr.empty 0 0 in
            encode_z { e with z }
        | d ->
            H.N.dst e.h e.t 0 (Bstr.length e.t);
            encode_h { e with z } d)

  and encode_h e d =
    let v, d = match d with v :: d -> (v, d) | [] -> (`End, []) in
    match (H.N.encode e.h v, d) with
    | `Ok, [] ->
        let len = Bstr.length e.t - H.N.dst_rem e.h in
        let z = Zl.Def.src e.z e.t 0 len in
        encode_z { e with d; z }
    | `Ok, d -> encode_h { e with d } d
    | `Partial, d ->
        let len = Bstr.length e.t - H.N.dst_rem e.h in
        let z = Zl.Def.src e.z e.t 0 len in
        encode_z { e with d= `Await :: d; z }

  let encode e = encode_z e

  let encoder ?(level = 4) ~i ~q ~w ~source src dst hunks =
    let o, o_pos, o_max =
      match dst with
      | `Manual -> (Bstr.empty, 1, 0)
      | `Buffer _ -> (Bstr.create De.io_buffer_size, 0, De.io_buffer_size - 1)
    in
    let z = Zl.Def.encoder `Manual `Manual ~q ~w ~level in
    let z = Zl.Def.dst z Bstr.empty 0 0 in
    let dst_len = Bstr.length src in
    let fn = function
      | Duff.Copy (off, len) -> `Copy (off, len)
      | Duff.Insert (off, len) -> `Insert (Bstr.sub_string src ~off ~len)
    in
    {
      dst
    ; o
    ; o_pos
    ; o_max
    ; t= i
    ; d= List.map fn hunks
    ; z
    ; h= H.N.encoder `Manual ~dst_len ~src_len:source
    }

  let dst_rem e = e.o_max - e.o_pos + 1

  let dst e s j l =
    let z = Zl.Def.dst e.z s j l in
    { e with z; o= s; o_pos= j; o_max= j + l - 1 }
end

module M : sig
  type decoder
  type src = [ `String of string | `Manual ]

  type decode =
    [ `Await of decoder
    | `Header of int * int * decoder
    | `End of decoder
    | `Malformed of string ]

  val src_len : decoder -> int
  val dst_len : decoder -> int
  val src_rem : decoder -> int
  val dst_rem : decoder -> int
  val src : decoder -> Bstr.t -> int -> int -> decoder
  val dst : decoder -> Bstr.t -> int -> int -> decoder
  val source : decoder -> Bstr.t -> decoder
  val decode : decoder -> decode

  val decoder :
    ?source:Bstr.t -> o:Bstr.t -> allocate:(int -> Zl.window) -> src -> decoder
end = struct
  type src = [ `String of string | `Manual ]

  type decoder = {
      src: src
    ; dst_len: int
    ; src_len: int
    ; o: Bstr.t
    ; z: Zl.Inf.decoder
    ; h: H.M.decoder
    ; k: decoder -> decode
  }

  and decode =
    [ `Await of decoder
    | `Header of int * int * decoder
    | `End of decoder
    | `Malformed of string ]

  let refill k d =
    match d.src with
    | `String _ ->
        let z = Zl.Inf.src d.z Bstr.empty 0 0 in
        k { d with z }
    | `Manual -> `Await { d with k }

  let rec decode d =
    match H.M.decode d.h with
    | `Header (src_len, dst_len) ->
        `Header (src_len, dst_len, { d with src_len; dst_len; k= decode })
    | `End -> `End { d with k= decode }
    | `Malformed err -> `Malformed err
    | `Await -> inflate { d with z= Zl.Inf.flush d.z }

  and inflate d =
    match Zl.Inf.decode d.z with
    | `Await z ->
        let dst_len = Bstr.length d.o - Zl.Inf.dst_rem z in
        H.M.src d.h d.o 0 dst_len;
        refill inflate { d with z }
    | `End z ->
        let dst_len = Bstr.length d.o - Zl.Inf.dst_rem z in
        H.M.src d.h d.o 0 dst_len;
        decode { d with z }
    | `Flush z ->
        let dst_len = Bstr.length d.o - Zl.Inf.dst_rem z in
        H.M.src d.h d.o 0 dst_len;
        decode { d with z }
    | `Malformed err -> `Malformed err

  let src d s j l =
    let z = Zl.Inf.src d.z s j l in
    { d with z }

  let dst d bstr off len = H.M.dst d.h bstr off len; d
  let source d src = H.M.source d.h src; d

  let dst_len d =
    let dst_len = H.M.dst_len d.h in
    assert (d.dst_len = dst_len);
    dst_len

  let src_len d =
    let src_len = H.M.src_len d.h in
    assert (d.src_len = src_len);
    src_len

  let dst_rem d = H.M.dst_rem d.h
  let src_rem d = Zl.Inf.src_rem d.z

  let decoder ?source ~o ~allocate src =
    let decoder_z = Zl.Inf.decoder `Manual ~o ~allocate in
    let decoder_h = H.M.decoder `Manual ?source in
    { src; dst_len= 0; src_len= 0; o; z= decoder_z; h= decoder_h; k= decode }

  let decode d = d.k d
end
