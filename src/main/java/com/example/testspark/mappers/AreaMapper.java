package com.example.testspark.mappers;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serial;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class AreaMapper implements MapFunction<Row, Double>
{

    @Serial
    private static final long serialVersionUID = 45679L;

    @Override
    public Double call(Row row) throws Exception {

        var point1 = new Tuple2<>(row.getStruct(0).getInt(0), row.getStruct(0).getInt(1));
        var point2 = new Tuple2<>(row.getStruct(1).getInt(0), row.getStruct(1).getInt(1));
        var point3 = new Tuple2<>(row.getStruct(2).getInt(0), row.getStruct(2).getInt(1));
        var point4 = new Tuple2<>(row.getStruct(3).getInt(0), row.getStruct(3).getInt(1));

        var pointList = Arrays.asList(point1, point2, point3, point4);
        return calculatingAreaUsingTheGaussFormula(pointList);
    }

    /**
     * Для вычисления площади простого многоугольника с любым количеством вершин, представленных в виде списка координат,
     * при последовательном обходе которых, не образуются пересекающиеся линии, применяется формула Гаусса
     */
    private static double calculatingAreaUsingTheGaussFormula(List<Tuple2<Integer, Integer>> pintList) {
        try {
            int xySum = 0;
            int yxSum = 0;
            for (int i = 0; i < pintList.size(); i++) {
                if (i == pintList.size() - 1) {

                    var point1 = pintList.get(i);
                    var point2 = pintList.get(0);

                    xySum = xySum + (point1._1 * point2._2);
                    yxSum = yxSum + (point1._2 * point2._1);
                    break;
                }
                var point1 = pintList.get(i);
                var point2 = pintList.get(i + 1);

                xySum = xySum + (point1._1 * point2._2);
                yxSum = yxSum + (point1._2 * point2._1);
            }
            int mySum = xySum - yxSum;
            mySum = mySum < 0 ? -1 * mySum : mySum;
            return (double) mySum / 2;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * Метод сортирует точки которые образуют не правильный четырехугольник
     * Метод находит сначало крайнюю нижнюю левую точку и крайнюю правую верхнюю точку
     * Далее находим центральнцю верхнюю точку и центральную нижнюю точку
     * Записываем все это в порядке: нижняя левая, центральная верхняя правая верхняя нижняя центральная
     * При обходе отчек по порядку получится обход фигуры по часовой стрелке
     * @param pintList не отсортированый список точек
     * @return возвращаем отсортированые точки
     */
    private static List<Tuple2<Integer, Integer>> creatingTheSidesOfFigure(List<Tuple2<Integer, Integer>> pintList) {

        var copyArr = new ArrayList<>(pintList);

        Tuple2<Integer, Integer> leftmostPoint = null;
        Tuple2<Integer, Integer> rightmostPoint = null;
        Tuple2<Integer, Integer> midpointTop = null;
        Tuple2<Integer, Integer> midpointBottom = null;

        var sortArr = new ArrayList<Tuple2<Integer, Integer>>();
        var xSort = copyArr.stream().sorted(Comparator.comparing(Tuple2::_1)).toList();
        //поиск точки начала посторения фигуры
        //если минимальные значения по x равны берем минимальное из них по y
        if (Objects.equals(xSort.get(0)._1, xSort.get(1)._1)) {
            leftmostPoint = Stream.of(xSort.get(0), xSort.get(1)).min(Comparator.comparing(Tuple2::_1)).get();
        } else {
            leftmostPoint = xSort.get(0);
        }
        //поиск точки окончания постороения фигуры
        //если максимальные значения по x равны берем максимальное из них по y
        if (Objects.equals(xSort.get(2)._1, xSort.get(3)._1)) {
            rightmostPoint = Stream.of(xSort.get(2), xSort.get(3)).max(Comparator.comparing(Tuple2::_1)).get();
        } else {
            rightmostPoint = xSort.get(3);
        }
        copyArr.removeAll(Arrays.asList(leftmostPoint, rightmostPoint));
        if (copyArr.size() == 0 || copyArr.size() == 1) {
            //треугольник или вообще прямая нам не подойдут
            return new ArrayList<>();
        }
        //поиск промежуточной верхней точки
        midpointTop = copyArr.stream().max(Comparator.comparing(Tuple2::_2)).get();
        //поиск помежуточной нижний точки
        midpointBottom = copyArr.stream().min(Comparator.comparing(Tuple2::_2)).get();

        //обход идет по часовой стрелке
        sortArr.add(leftmostPoint);
        sortArr.add(midpointTop);
        sortArr.add(rightmostPoint);
        sortArr.add(midpointBottom);

        return sortArr;
    }

    /**
     * Нахрждкеие длинны вектора по координатам используя формулу d^2= (х2— х1)^2+ (y2— y1)^2
     * @param start координаты x и y начала вектора
     * @param end координаты x и y конца вектора
     * @return возвращает длинну вектора в формате double либо -1
     */
    private static double vectorLengthCalculation(Tuple2<Integer, Integer> start, Tuple2<Integer, Integer> end) {
        if (start == null || end == null) {
            return -1;
        }
        try {
            return Math.sqrt((end._1 - start._1)^2 + (end._2 - start._2)^2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    /*
    public static void main(String[]args) {
        var list = new ArrayList<Tuple2<Integer, Integer>>();
        list.add(new Tuple2<>(2,2));
        list.add(new Tuple2<>(2,4));
        list.add(new Tuple2<>(1,10));
        list.add(new Tuple2<>(1,9));
        var sortList = creatingTheSidesOfFigure(list);
        sortList.forEach(i -> System.out.println("x = " + i._1 + " y = " + i._2));
        var area = calculatingAreaUsingTheGaussFormula(sortList);
        System.out.println(area);
    }
     */
}
