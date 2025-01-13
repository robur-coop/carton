# Carton, the PACKv2 implementation in OCaml

A PACK file is a file containing compressed “objects”. In the Git context,
these objects are commits, trees, blobs and tags. The advantage of the PACK
format is that you can compress these objects together, while retaining the
possibility of extracting them almost independently of the others: this is
known as _“random” access_ to these objects.

We therefore have the advantage of compression (the size of the PACK file is
comparable to a `*.tar.gz` archive) and the benefit of extracting these objects
without necessarily inflating the previous ones.

The PACK file is a format used by Git that you can find in your Git
repositories at “.git/objects/pack/”. This is what is transmitted when you clone
a repository.

Carton is a small library for manipulating and generating PACK files produced
by Git and/or tweaked by the user for another use (such as storing your mails).
This library is independent of any scheduler (lwt, miou, etc.) and can easily be
extended for other schedulers. However, support for lwt and miou is available.

## Format

As part of the larger OCaml email project, and as the current Git specification
remains rather obscure, documentation on the format is also available.

## Tools

The distribution offers several tools for manipulating a PACK file. Here are a
few examples of how to use these tools.
```shell
$ git clone https://github.com/robur-coop/carton.git
$ cp carton/.git/objects/pack/pack-*.pack pack.pack
$ carton index pack.pack
$ carton verify pack.idx
17c2336bccb3b4fbd6eb430bf5fe1c4f1f8184e3 commit 12
6da5bd47b5a2ce2ca0620ce33d654318a2dac423 commit 277
20065a67b5497761ee1fb3ed91eb49fb9f6944f9 commit 484
7ec30cf371a6a7fe14502a34712ea91958737dbe commit 777
...
$ carton get pack.idx 17c2336bccb3b4fbd6eb430bf5fe1c4f1f8184e3
kind:         a                      
length:       407 byte(s)
depth:        1
cache misses: 1
cache hits:   3
tree:         0000000c

00000000: 7472 6565 2030 3664 3033 3934 6235 3439  tree 06d0394b549
00000010: 3731 3535 3566 6238 3134 3933 6538 3633  71555fb81493e863
...
$ mkdir pack
$ carton explode "pack/%s/%s" new.pack > entries.pack
$ carton make -n $(cat entries.pack | wc -l) -e entries.pack new.pack
$ carton index new.pack
$ carton get new.idx 17c2336bccb3b4fbd6eb430bf5fe1c4f1f8184e3
...
```

These tools are actuallly not designed to be used in production, but any
feedback (such as the discovery of bugs) is appreciated to improve them.

These tools use Miou as a scheduler to take advantage of parallelism as early
as possible, especially when it comes to calculating and checking PACK files.
This is where the `carton-miou-unix` package comes in. However, as mentioned
above, the core of Carton is independent of any scheduler. A derivation with
lwt is also available, but does not offer the tools shown above. `carton-lwt`
can, however, be used as a library to manipulate PACK files.

## Benchmarks

We can currently compare Carton and Git, mainly on the verification of a PACK
file which requires all the objects to be loaded (and patches applied). This is
certainly the most expensive operation with the generation of a PACK file.

Furthermore, checking a large PACK file such as the one you can obtain by
cloning ocaml/ocaml produces this result:
```shell
$ opam install carton-miou-unix
$ hyperfine "git verify-pack ..." "pack verify -q --threads 4 ..."
Benchmark 1: git verify-pack ...
  Time (mean ± σ):      5.854 s ±  0.042 s    [User: 21.721 s, System: 0.882 s]
  Range (min … max):    5.780 s …  5.935 s    10 runs
 
Benchmark 2: carton.verify -q --threads 4 ...
  Time (mean ± σ):     14.438 s ±  0.081 s    [User: 35.073 s, System: 2.906 s]
  Range (min … max):   14.297 s … 14.586 s    10 runs
 
Summary
  git verify-pack ...
    2.47 ± 0.02 times faster than carton.verify -q --threads 4 ...
```

As you can see, the pure OCaml implementation is 2 times slower than the C
implementation. There are several reasons for this - the first is that most of
the basic operations such as decompression are done in OCaml (and not in C).
Other parameters such as GC can also be considered.

However, it's worth noting that Carton isn't that slow! What's more, even if it
would be difficult to approach the performance of a C program, OCaml gives us
certain guarantees such as typing and bound-checking.
