## ⚙️ Prerequisites

- Java 8 (JDK 1.8)
- Maven 3.x
- Spark 2.3.4
- Hadoop `winutils.exe` for Windows
- Set environment variable `HADOOP_HOME`

Example:

```bash
set HADOOP_HOME=C:\hadoop
set PATH=%HADOOP_HOME%\bin;%PATH%

set JAVA_HOME=C:\Program Files\Java\jdk1.8.0_xx

set PATH=%JAVA_HOME%\bin

set HADOOP_HOME = C:\hadoop

set PATH=%HADOOP_HOME%\bin

verify => C:\hadoop\bin\winutils.exe

mkdir C:\tmp\hadoop-{yourusername}

Run in CMD => winutils.exe chmod 777 C:\tmp\hadoop-{yourusername}

RUN in CMD => winutils.exe chmod 777 C:\hadoop\bin\winutils.exe

