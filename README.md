# IPC
Word-Count Problem mithilfe von Map-Reduce in verschiedenen IPC Verfahren.

**1. Kompilieren**
```bash
mvn package
```

**2. Ausführen**
```bash
# java -jar ./target/ipc.jar [method] [#Clients] [chunkSize] [filePath]
# method = [threads,pipes,np,tcp,uds]
java -jar ./target/ipc.jar threads 4 40000 ./texts/mobydick.txt
```

zum testen auch eine Singlemethode
```bash
# java -jar ./target/ipc.jar single [#chunkSize] [#filePath]
java -jar ./target/ipc.jar single 40000 ./texts/mobydick.txt
```

**3. Beispielausgabe**
```text
❯ java -jar ./target/ipc.jar threads 4 40000 ./texts/mobydick.txt
the: 14715
of: 6746
and: 6513
a: 4799
to: 4709
in: 4241
that: 3081
it: 2535
his: 2530
i: 2120
Runtime: 0.516261427 s

```

**4. Testscript**
Ein Skript zum Testen aller Verfahren bei eingabe von Anzahl der Clients, ChunkSize und Filepath. Ergebnis wird in result.csv gespeichert und kann in Excel importiert werden.
```bash
./test.sh [#Clients] [chunkSize] [filePath]
./test.sh  4 40000 ./texts/mobydick.txt
```

![heh](https://cdn.epicstream.com/images/ncavvykf/epicstream/89bba69c108a1c7a718b8e3cb8831e6fba8925da-1920x1080.jpg?rect=0,36,1920,1008&w=1200&h=630&auto=format)
