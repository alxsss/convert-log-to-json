How to Install and Compile

1. Checkout the code by git clone https://github.com/alxsss/convert-log-to-json.git
2. cd convert-log-to-json
3. mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.ConvertLogToJson \
     -Dexec.args="--inputFile=input.txt --output=archive --output=output/routeone --output=output/routetwo" -Pdirect-runner
4. There must be three output options in the previous command, one for archive folder and the other two for routeone and routetwo