package hello;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@RestController
public class DemoApplication {

    @Autowired
    Consumer consumer;

    @Autowired
    public volatile SuperMap superMap;

    @Autowired
    JavaSparkContext javaSparkContext;

    @Value("${consumer.brokers}")
    private String broker;

    @Value("${consumer.topics}")
    private String topic;

    @PostConstruct
    public void init() throws ExecutionException, InterruptedException {

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,broker);

        AdminClient admin = AdminClient.create(config);

        NewTopic newTopic = new NewTopic(topic,1,(short) 1);
        CreateTopicsResult createTopic = admin.createTopics(Collections.singleton(newTopic));
        KafkaFuture<Void> all = createTopic.all();
        try{all.get();}catch (Exception e){
            System.out.println("error");
        }
        System.err.println("topic created");

        consumer.start();
    }

    @RequestMapping("/")
    public ModelAndView index() {
        ModelAndView model = new ModelAndView("index");
        model.addObject("items",javaSparkContext.parallelize(superMap.getList()).mapToPair(tuple2->tuple2)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .map(Tuple2->Tuple2._2+" "+Integer.toString(Tuple2._1))//mapToPair(Tuple2::swap) для визуализации мб
                .take(30));
        return model;
    }

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

}
