package com.hanss.acc;

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class EProcessor implements Processor<String, String> {
    private ProcessorContext context;
    private KeyValueStore<String, Integer> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        this.context.schedule(1000);
        this.kvStore = (KeyValueStore<String, Integer>)context.getStateStore("Counts");//ensure Topic
    }


    @Override
    public void process(String key, String value) {
        Stream.of(value.toLowerCase().split(" ")).forEach((String thisEnergyValue) -> {
          //System.out.println(thisEnergyValue+" --------");
          //if(thisEnergyValue=="mev")return;          
          //thisEnergyValue = thisEnergyValue.replace("新增alpha粒子束:","");
          //thisEnergyValue = thisEnergyValue.replace("新增alpha粒子束:","");
          //System.out.println(thisEnergyValue);
          //if(thisEnergyValue.equals("mev"))return;
          thisEnergyValue   = Integer.toString(( new Double(Double.valueOf(thisEnergyValue).doubleValue()) ).intValue()); 
          //System.out.println(thisEnergyValueInt);
          //Optional<Integer> counts = Optional.ofNullable(kvStore.get(thisEnergyValue));
          //int count = counts.map(EnergyCount -> EnergyCount + 1).orElse(1); 
          Integer count = this.kvStore.get(thisEnergyValue);  
          if (count == null) {  
              this.kvStore.put(thisEnergyValue, 1);  
          } else {  
          	  count = count+1;
              this.kvStore.put(thisEnergyValue, count);  
          }
          System.out.println(thisEnergyValue+" : "+count);
          //this.kvStore.put(thisEnergyValue, count);
        }); //regard the Lamda as a f(*)
        //Optional!:
    }

    //---------------------------------------------------
    @Override
    public void process(String key, String value) {
        String thisEnergyValue   = Integer.toString(( new Double(Double.valueOf(value).doubleValue()) ).intValue()); 
        System.out.println(value+" : "+thisEnergyValue);
        this.kvStore.put(value, thisEnergyValueInt);
    }

    @Override
    public void punctuate(long timestamp) {
        try (KeyValueIterator<String, Integer> iterator = this.kvStore.all()) {
            iterator.forEachRemaining(entry -> {
              context.forward(entry.key, entry.value);//with "final ProcessorNode previousNode = currentNode();" for move forward and to delete
              //String thisEnergy  = entry.value;
              //int energyToDelete =( new Double(Double.valueOf((thisEnergy).replace(" Mev","")).doubleValue()) ).intValue(); 
              this.kvStore.delete(entry.key);//delete
            });
        }
        context.commit();
    }

    @Override
    public void close() {
        this.kvStore.close();
    }
}

[root@master AccBak]# mvn exec:java -Dexec.mainClass="com.hanss.acc.KafkaClient"
[INFO] Scanning for projects...
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] Building Acc 1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO] 
[INFO] --- exec-maven-plugin:1.6.0:java (default-cli) @ Acc ---
log4j:WARN No appenders could be found for logger (kafka.utils.VerifiableProperties).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
P1 发送了消息->[探测到粒子: 1733.223  Mev]
P1 消息主题：粒子信息收集
[WARNING] 
java.lang.NullPointerException
	at java.lang.String.<init>(String.java:566)
	at com.hanss.acc.JConsumer.run(JConsumer.java:54)
EP2 正在计算
EP2 计算散射能量期望 : 888 Mev
SP3 收到计算结果： null Mev
SP3 收到计算结果： null Mev
SP3 收到计算结果： null Mev
SP3 收到计算结果： null Mev
SP3 收到计算结果： null Mev
SP3 收到计算结果： null Mev
SP3 收到计算结果： null Mev
SP3 收到计算结果： 888 Mev
SP3 收到计算结果： null Mev
SP3 收到计算结果： null Mev
SP3 收到计算结果： null Mev
SP3 收到计算结果： null Mev
P1 发送了消息->[探测到粒子: 569.331  Mev]
P1 消息主题：粒子信息收集
EP2 正在计算
EP2 计算散射能量期望 : 306 Mev
SP3 收到计算结果： 306 Mev
P1 发送了消息->[探测到粒子: 667.824  Mev]
P1 消息主题：粒子信息收集
EP2 正在计算
EP2 计算散射能量期望 : 355 Mev
SP3 收到计算结果： 355 Mev
P1 发送了消息->[探测到粒子: 1217.001  Mev]
P1 消息主题：粒子信息收集
EP2 正在计算
EP2 计算散射能量期望 : 630 Mev
SP3 收到计算结果： 630 Mev
P1 发送了消息->[探测到粒子: 1733.223  Mev]
P1 消息主题：粒子信息收集
EP2 正在计算
EP2 计算散射能量期望 : 888 Mev
SP3 收到计算结果： 888 Mev
P1 发送了消息->[探测到粒子: 569.331  Mev]
P1 消息主题：粒子信息收集
EP2 正在计算
EP2 计算散射能量期望 : 306 Mev
SP3 收到计算结果： 306 Mev
^C[root@master AccBak]# 

