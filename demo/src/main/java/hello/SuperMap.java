package hello;

import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;


@Component
public class SuperMap implements Serializable {
    //atomic?
    private volatile Map<String,Integer> map;

    public SuperMap() {
        this.map = Collections.synchronizedMap(new HashMap<>());
    }
    public void add(Tuple2<String,Integer> tuple){
        if(map.containsKey(tuple._1))
            map.put(tuple._1,map.get(tuple._1)+tuple._2);
        else
            map.put(tuple._1,tuple._2);
    }
    public List<Tuple2<String,Integer>> getList(){
        List list = new ArrayList();
        map.forEach((k,v) -> list.add(new Tuple2<>(k,v)));
        System.out.println(list.size());
        return list;
    }
}
