## 스파크 설치

- brew install scala@2.11

- intellij > maven > scala-archetype

- pom.xml

  - scala-core 추가

- package 실행

  - 스칼라 버전, dependency 등의 문제를 알려줌.

  - ```
     스칼라 버전이 3.1이었는데 2.11로 바꿔줘야 했음.
     Expected all dependencies to require Scala version: 2.11.7
     sparkcollection:scala-park-sample:1.0-SNAPSHOT requires scala version: 2.11.7
     org.scala-lang.modules:scala-xml_2.11:1.0.6 requires scala version: 2.11.7
     com.twitter:chill_2.11:0.8.4 requires scala version: 2.11.7
     org.apache.spark:spark-core_2.11:2.3.4 requires scala version: 2.11.7
     org.json4s:json4s-jackson_2.11:3.2.11 requires scala version: 2.11.7
     org.json4s:json4s-core_2.11:3.2.11 requires scala version: 2.11.7
     org.json4s:json4s-ast_2.11:3.2.11 requires scala version: 2.11.7
     org.json4s:json4s-core_2.11:3.2.11 requires scala version: 2.11.0
    Multiple versions of scala libraries detected!
    ```

  - 스칼라 2.11 설치

    - brew install scala@2.11
    - echo 'export PATH="/usr/local/opt/scala@2.11/bin:$PATH"' >> ~/.zshrc
    - scala -version

- 실행

  - Jar 파일을 namenode에 옮긴 뒤 ~/script/spark-submit.sh 실행.

    - ftp로 옮길때 binary로 upload 해줘야 파일이 깨지지 않는다.

      ```sh
      ftp> type binary 
      ```
      
- 빌드

  - Maven 설치

    - wget http://apache.mirror.cdnetworks.com/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz

    - tar -xvzf ./apache-maven-3.6.3-bin.tar.gz, 심볼릭 링크 연결 -> ~/apps/maven3

    - bash_profile

    - ```shell
      PATH=$PATH:$HOME/:$HOME/bin:$MAVEN_HOME/bin
      ```

    - mvn -version

  - Maven 빌드

    - mvn package -Dmaven.test.skip=true