package atguigu;

import java.util.ArrayList;
import java.util.List;

public class Test {

    public static void main(String[] args) {
        String test = "asd";
        test += 'a';
        System.out.println(test);

        List<List<Integer>> test2 = new ArrayList<>();
        List<Integer> test3 = new ArrayList<>();
        test3.add(1);
        test3.add(2);
        test3.add(3);
        test3.add(4);
        List<Integer> path = new ArrayList<>(test3);
        test3.toArray();
        test2.add(path);

        System.out.println(test2);
        test3.remove(3);
        System.out.println(test2);

    }
}
