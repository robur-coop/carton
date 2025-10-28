type entry =
  [ `Number of int
  | `Inflate of (Carton.Kind.t * int) option * string
  | `Entry of Carton.First_pass.entry
  | `Hash of string ]

val first_pass :
  digest:Carton.First_pass.digest -> ref_length:int -> (string, entry) Flux.flow

val oracle :
  identify:'ctx Carton.First_pass.identify -> (entry, Carton.oracle) Flux.sink

val entries :
     ?threads:int
  -> 'fd Carton.t
  -> Carton.oracle
  -> (Carton.Value.t * Carton.Uid.t) Flux.source
