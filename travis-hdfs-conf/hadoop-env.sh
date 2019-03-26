# Fix "java.lang.NoClassDefFoundError: javax/activation/DataSource" on newer Java versions
export HADOOP_OPTS="--add-modules java.activation"
for jh in /usr/lib/jvm/java-10-openjdk /usr/local/lib/jvm/openjdk10
do
    if test -d "$jh"
    then
        export JAVA_HOME="$jh"
        break
    fi
done
