#!/usr/bin/env ocaml

let () =
  let rec go acc =
    match input_line stdin with
    | exception End_of_file -> Format.printf "%d\n%!" acc
    | _line -> go (succ acc)
  in
  go 0
