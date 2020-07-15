The Rig
===

The Rig pipes `STDIN` to some programs listed in a `Rigfile` and presents their `STDOUT` and `STDERR`
in a single stream.

Usage
---

```shell
# report mode, for programs that produce document-like output
./rig.py report Rigfile

# stream mode, for programs that produce a continuos stream of text,
# like `tail` (use CTRL-C to stop)
./rig.py stream Rigfile
```

A `Rigfile` is a list of programs to execute, one per line and prefixed by a label.

```
cat > Rigfile <<EOF
host-1 psql -h host-1.example.invalid
host-2 psql -h host-2.example.invalid
EOF

echo 'SELECT * FROM some_table;' | rig.py report Rigfile
```

A file whose name ends with `.sh` is executed, and the output is used instead of the file
contents, eg.:

```
cat > Rigfile.sh <<EOF
#!/bin/sh
kubectl get pods -o name | sed 's:^pod/::' | while read -r POD
do
    echo "$POD" kubectl exec -i "$POD" bash
done
EOF

echo 'ls -l' | ./rig.py report Rigfile
```
