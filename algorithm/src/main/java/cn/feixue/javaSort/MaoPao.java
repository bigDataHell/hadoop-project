package cn.feixue.javaSort;

import java.util.Arrays;

/**
 * java冒泡排序实现
 */
public class MaoPao {

    public static void main(String[] args) {

        int[] arr = {4,3,6,45,2,1,89,-1,9,0};


        bubbleSort(arr);

        System.out.println(Arrays.toString(arr));

    }

    private static void bubbleSort(int[] arr) {
        if(arr.length <= 0){
            return;
        }

        for(int i = 0; i < arr.length -1; i++){
            for(int j = 0; j < arr.length-i-1; j++){
                if(arr[j] < arr[j+1]){
                    int temp = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1] = temp;
                }

            }
        }
    }
}
