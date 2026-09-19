  $ ./boite.exe roundtrip
  roundtrip: ok

  $ export BT=$(pwd)/boite.exe
  $ export HOME=$(pwd)
  $ export GIT_CONFIG_GLOBAL=/dev/null
  $ export GIT_CONFIG_SYSTEM=/dev/null
  $ export GIT_AUTHOR_NAME=carton
  $ export GIT_AUTHOR_EMAIL=carton@example.org
  $ export GIT_COMMITTER_NAME=carton
  $ export GIT_COMMITTER_EMAIL=carton@example.org
  $ export GIT_AUTHOR_DATE="2026-01-01T00:00:00+0000"
  $ export GIT_COMMITTER_DATE="2026-01-01T00:00:00+0000"

  $ mkdir repo
  $ git -c init.defaultBranch=main init -q repo
  $ echo hello > repo/a.txt
  $ git -C repo add a.txt
  $ git -C repo commit -q -m first
  $ echo world >> repo/a.txt
  $ git -C repo commit -q -am second
  $ git -C repo tag v1.0.0
  $ git -C repo bundle create ../all.bundle --all > /dev/null 2>&1
  $ git -C repo bundle create ../thin.bundle main~1..main > /dev/null 2>&1

  $ $BT info all.bundle
  version: 2
  ref-length: 20
  thin: false
  references: 3
  prerequisites: 0
  $ $BT list-heads all.bundle > carton.heads
  $ git -C repo bundle list-heads ../all.bundle > git.heads 2> /dev/null
  $ diff carton.heads git.heads
  $ cat carton.heads
  2cf3dc252385a12e1092540af768f754d56e5089 refs/heads/main
  2cf3dc252385a12e1092540af768f754d56e5089 refs/tags/v1.0.0
  2cf3dc252385a12e1092540af768f754d56e5089 HEAD

  $ $BT info thin.bundle
  version: 2
  ref-length: 20
  thin: true
  references: 1
  prerequisites: 1
  $ $BT list-prerequisites thin.bundle
  -fb4d9a48accb955b670aea3da995625468824d59 first

  $ $BT split all.bundle all.pack
  $ carton index all.pack
Offsets and CRC32 depend on the zlib used by Git (zlib, zlib-ng, etc.), so we
only keep the hash and the kind of objects.
  $ carton verify --without-progress --without-consumed all.idx | ocaml fields.ml
  2cf3dc252385a12e1092540af768f754d56e5089 commit
  fb4d9a48accb955b670aea3da995625468824d59 commit
  d4e01edf1e8aa72182ed9449e7d12b5e4df8b201 tree
  2e81171448eb9f2ee3821e3d447aa6b2fe3ddba1 tree
  94954abda49de8615a048f8d2e64b5de848e27a1 blob
  ce013625030ba8dba906f756967f9e9ca394464a blob

  $ oid=$(git -C repo rev-parse main)
  $ $BT create -o new.bundle -p all.pack -r refs/heads/main=$oid -r HEAD=$oid
  $ git -C repo bundle verify ../new.bundle > /dev/null 2>&1 && echo "new.bundle is ok"
  new.bundle is ok
  $ git clone -q new.bundle clone > /dev/null 2>&1
  $ git -C clone log --oneline
  2cf3dc2 second
  fb4d9a4 first

The same, with the PACK re-delta-ified by Carton
  $ $BT repack -o repack.bundle -p all.pack -r refs/heads/main=$oid -r HEAD=$oid
  $ git -C repo bundle verify ../repack.bundle > /dev/null 2>&1 && echo "repack.bundle is ok"
  repack.bundle is ok
  $ git clone -q repack.bundle reclone > /dev/null 2>&1
  $ git -C reclone log --oneline
  2cf3dc2 second
  fb4d9a4 first

  $ git -c init.defaultBranch=main init -q --object-format=sha256 repo256
  $ echo hello > repo256/a.txt
  $ git -C repo256 add a.txt
  $ git -C repo256 commit -q -m first
  $ git -C repo256 bundle create ../sha256.bundle --all > /dev/null 2>&1
  $ $BT info sha256.bundle
  version: 3
  ref-length: 32
  thin: false
  references: 2
  prerequisites: 0
  $ $BT list-heads sha256.bundle > carton256.heads
  $ git -C repo256 bundle list-heads ../sha256.bundle > git256.heads 2> /dev/null
  $ diff carton256.heads git256.heads
