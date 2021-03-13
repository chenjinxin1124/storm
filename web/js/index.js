


    function loadInit() {
        loadWebSocket_shop_ranking();
        loadWebSocket_x_time();
        loadWebSocket_all_amt()
        // 基于准备好的dom，初始化echarts图表
        var div_center_right = document.getElementById('div-center-right');


       // var div_top_left = document.getElementById('div-top-left');

        var cao_charts_right_1 = document.getElementById('cao_charts_right_1');
;        var cao_charts_right_2 = document.getElementById('cao_charts_right_2');
        var cao_charts_right_3 = document.getElementById('cao_charts_right_3');



        var cao_charts_left_1 =document.getElementById('cao_charts_left_1');
        var cao_charts_left_2 = document.getElementById('cao_charts_left_2');
        var cao_charts_left_3 = document.getElementById('cao_charts_left_3');


        var myChartContainer = function () {

            div_center_right.style.height = (window.innerHeight/3 - 50)+'px';
            cao_charts_right_3.style.height = (window.innerHeight/3 - 70)+'px';
            cao_charts_right_1.style.height = (window.innerHeight/3 - 50)+'px';
            //div_top_left.style.height = (window.innerHeight/3 - 50)+'px';


            cao_charts_right_2.style.height = (window.innerHeight/3 - 50)+'px';
            cao_charts_left_1.style.height = (window.innerHeight/3 - 50)+'px';
            cao_charts_left_2.style.height = (window.innerHeight/3 - 50)+'px';
            cao_charts_left_3.style.height = (window.innerHeight/3 - 70)+'px';

        };

        myChartContainer();
        var my_cao_charts_right_1 = echarts.init(cao_charts_right_1);
        var center_right = echarts.init(div_center_right);
        var myChart_bottom_right = echarts.init(cao_charts_right_3);

        var myChart_center_left = echarts.init(cao_charts_left_2);




        // 为echarts对象加载数据
        loadCao_top_right(my_cao_charts_right_1);
        loadCao_center_right(center_right);
        loadCao_bottom_right( myChart_bottom_right);
        loadCao_center_left(myChart_center_left);
        window.onresize = function () {
            myChartContainer();
            center_right.resize();
        };
    }

















