# PACKv2 format

This documentation is intended to help you understand the PACKv2 format as
implemented in the Carton project and compatible (or quasi-equivalent) with
Git's PACKv2 format. Git does not currently have formal documentation for this
format. Only the Git implementation can really serve as a reference and help to
understand the various assumptions that users can make to their advantage in
order to extract objects or generate PACK files.

# Introduction / Overview

The PACK format is how Git stores most of its primary repository data. Over the
lifetime of a repository, loose objects (if any) and smaller packs are
consolidated into larger pack(s). The PACK format is also used over-the-wire as
well as being a part of other container formats like Git bundles.

A PACK file has the advantage of reconciling two aspects in the storage of
non-modifiable information:
- quasi-direct extraction of objects, requiring only the calculation of an IDX
  file associated with the PACK file
- a compression rate comparable to zlib/gzip/etc.

It is therefore possible to store a great deal of information without taking up
too much disk space and ensuring that read access remains reasonably fast. It
should be noted that the PACK file can be seen as a read-only database.
Modifying information in a PACK file is too costly and the format is not
designed for such an operation.

A good example of the advantages of a PACK file is the Linux kernel repository.
It concentrates almost 20 years of commits in a file of just 5 GB. All the
commits, files and tags are included. With 5 GB, you have the entire evolution
of the kernel over the last 20 years. A PACK file can be analysed to produce an
IDX file. This allows you to associate the hash of your objects (such as your
commits) with their positions in the PACK file. In this way, it is possible to
have quasi-direct access to objects according to their hashes. This involves
searching for the position in the IDX file (which is very fast) and then
extracting the object according to its position in the PACK file.

Thanks to this pairing between the PACK file and the IDX file, obtaining an
object according to its hash only takes a few milliseconds:
```shell
$ ls -sh pack.pack
5.1G pack.pack
$ carton index pack.pack
$ time carton get pack.idx 1da177e4c3f41524e886b7f1b8a0c1fc7321cac2
0.00s user 0.03s system
```

Please note that calculating the IDX file can take a long time. It consists of
calculating the hash of all the objects in the PACK file (in our example,
around 10 million). It requires, at the very least, analysing (in a serialised
way) the entire PACK file — what we call the first phase. It is only after this
initial analysis that we can parallelize the reconstruction of all our objects
and particularly those that are _doubly_ compressed — this is known as the
second phase.

To achieve a level of compression close to that of conventional algorithms,
objects can be compressed together in the form of "patches". This is why we say
that the extraction of an object is quasi-direct. This extraction may involve
us extracting other objects in order to extract the requested object (because
it is stored in the form of a patch in relation to these other objects).

The aim of this document is to explain the PACK format and to understand the
various algorithms underlying the format, so that other developers can
implement this format in a language. This document is associated with the
Carton project, which is an implementation of this format in OCaml. A few
examples will be given in OCaml in order to propose implementations of certain
details of the PACK format.

## Notational Conventions

### Requirements Notation

This document occasionally uses terms that appear in capital letters. When the
terms "MUST", "SHOULD", "RECOMMENDED", "MUST NOT", "SHOULD NOT", and "MAY"
appear capitalized, they are being used to indicate particular requirements of
this specification. A discussion of the meanings of these terms appears in
[RFC2119][RFC2119].

Commonly used terms in this document are described below.

PACK: the file format described in this document.

Object: a unit of bytes with a type. In the case of Git, we speak of Git
  objects such as a commit, a tree, a blob or a tag. These objects, whatever
  their meaning, can be serialized into a series of bytes as their own unit.

Reference: a unique identifier for an object that can be determined by the
  object itself (such as a hash algorithm applied to the series of bytes
  representing the object). As far as Git is concerned, the hash can be
  calculated for a file in this way: 

```shell
$ (echo -ne "blob `wc -c < $file`\0"; cat $file) | sha1sum
```

  Carton allows users to specify their own method for calculating this unique
  identifier.

Offset: a particular position in the PACK file.

Patch or Diff: A difference/patch between two objects, one of which is
  identified as the _source_ and the other as the _target_. It is then possible
  to reconstruct the _target_ object from the patch and the _source_ object.

Source: an object used in conjunction with a patch/diff to build a new so-called
  target object.

Target: an object built from another source object and a patch/diff.

Depth: the depth of an object corresponds to the number of patches required to
  build it. It's possible that an object needs a source, which in turn needs
  another source. In such a case, the depth of our first object would be 2.

Base object: serialized object requiring no other object to be extracted.

Delta object: object serialized in the form of a patch and therefore requiring
  a source.

### Notational Conventions

The order of transmission of the header and data described in this document is
resolved to the octet level. Whenever a diagram shows a group of octets, the
order of transmission of those octets is the normal order in which they are
read in English. For example, in the following diagram, the octets are
transmitted in the order they are numbered.

```
     0               1
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |       1       |       2       |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |       3       |       4       |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |       5       |       6       |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

Whenever an octet represents a numeric quantity, the left most bit in the
diagram is the high order or most significant bit. That is, the bit labeled 0
is the most significant bit. For example, the following diagram represents the
value 170 (decimal).

```
     0 1 2 3 4 5 6 7
    +-+-+-+-+-+-+-+-+
    |1 0 1 0 1 0 1 0|
    +-+-+-+-+-+-+-+-+
```

Similarly, whenever a multi-octet field represents a numeric quantity the left
most bit of the whole field is the most significant bit. When a multi-octet
quantity is transmitted the most significant octet is transmitted first.

## Header of PACK files

A PACK file always starts with 3 pieces of information:
- a magic number that identifies the file as a PACK file
- the version of the PACK file (currently version 2)
- the number of objects in the PACK file

Let's take the example of a PACKv2 file containing 3735928559 objects. You
should have 12 bytes at the beginning of the PACK file, corresponding to the
header:
- the magic number 0x5041434b (PACK)
- the version 0x00000002
- the number 3735928559 encoded in big-endian

```
 0               1               2               3
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       P       |       A       |       C       |       K       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       0       |       0       |       0       |       2       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      0xde     |      0xad     |      0xbe     |      0xef     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

In OCaml, header generation could correspond to:
```ocaml
let hdr = Bytes.make 12 '\000' in
Bytes.set_int32_be hdr 0 0x5041434bl;
Bytes.set_int32_be hdr 4 2l;
Bytes.set_int32_be hdr 8 0xdeadbeefl
```

## Entries

A PACK file can contain 6 types of object:
- 4 types of objects called _base objects_
- an object requiring a source available at a precise position to build itself,
  and saved as a patch: `OBJ_OFS_DELTA`
- an object requiring a source identified by its reference and registered in
  the form of a patch: `OBJ_REF_DELTA`

These 6 types start with a header that indicates the size of the object
**after** decompression. For _base objects_, this size corresponds to the
object's actual size. Otherwise, for `OBJ_OFS_DELTA` and `OBJ_REF_DELTA`
objects, it corresponds to the patch size. After this header, you will find the
compressed object/patch (using zlib).

An object (or an entry in the PACK file) begins with a header. The header is a
series of bytes, the first of which informs you of the object's type, and the
rest of which informs you of the object's size. The object type is encoded in
the 3 least significant bits of the first byte. The size is encoded in the form
of a length variable.

```
     0 1 2 3 4 5 6 7
    +-+-+-+-+-+-+-+-+
    |S S S S S K K K|
    +-+-+-+-+-+-+-+-+
```

K corresponds to the type of object:
- `0b001` is a _base object_ of type A (commit)
- `0b010` is a _base object_ of type B (tree)
- `0b011` is a _base object_ of type C (blob)
- `0b100` is a _base object_ of type D (tag)
- `0b110` is a `OBJ_OFS_DELTA`
- `0b111` is a `OBJ_REF_DELTA`

The type of patches is determined by their source. In other words, if an
`OBJ_OFS_DELTA` requires a _base object_ of type A, the former is also of type
A. This implies a rule in patch generation: a patch can only be generated from 2
objects of the same type.

### Variable-length


