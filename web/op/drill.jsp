<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%
    String path = request.getContextPath();
    String basePath = request.getScheme()+"://"+request.getServerName()
            +":"+request.getServerPort()+path+"/";
%>
<html>
<head>
    <base href=" <%=basePath%>">
</head>
<link rel="stylesheet" type="text/css" media="screen" href="lib/css/redmond/jquery-ui-1.10.3.custom.min.css" />
<link rel="stylesheet" type="text/css" media="screen" href="lib/css/ui.jqgrid.css" />
<link href="lib/ligerUI/skins/Aqua/css/ligerui-all.css" rel="stylesheet" type="text/css" />
<script src="lib/jquery/jquery-1.9.1.min.js" type="text/javascript" ></script>
<script src="lib/ligerUI/js/ligerui.all.js" type="text/javascript"></script>
<script src="lib/grid.locale-cn.js" type="text/javascript"></script>
<script src="lib/jquery/jquery.jqGrid.min.js" type="text/javascript"></script>


<style type="text/css">
    body{ padding:10px; margin:0; font-size:12px;}
    .l-table-edit-td{ padding:2px;}
    .l-table-edit {font-size:13px;}
    #layout1{  width:100%; margin:40px;  height:700px;
        margin:0; padding:0;}
</style>
<body>
<div position="center" title="查询结果" id="center">
    <table id="searchgrid"></table>
</div>
<script type="text/javascript">

    $(function (){

        loadData();

        $(window).bind('resize',resize);
        resize();
    });


    function resize(){
        $('#searchgrid').setGridWidth($('#center').width()*0.99).setGridHeight($('#center').height()*0.95);
    }

    function loadData(){

        $.ligerDialog.waitting('正在检测中,请稍候...');
        $.ajax({
            url: 'op',
            type: 'post',
            dataType: 'json',
            data: {"action":'dload'},
            success: function(data){
                if(data.success == "true"){
                    $("#searchgrid").jqGrid("clearGridData");
                    $("#searchgrid").addRowData("1",data.result,"first");
                }else{
                    alert("检测失败，" + data.result);
                }
                $.ligerDialog.closeWaitting();
                console.info(data);
            },
            error: function(xmlResponse){
                alert("检测失败" + xmlResponse.responseText);
                $.ligerDialog.closeWaitting();
            }
        });
    }

    $("#searchgrid").jqGrid({
        datatype: "local",
        colNames:['节点','状态','操作'],
        colModel:[
            {index:'name',name:'name',align:"center"},
            {index:'status',name:'status',align:"center",formatter:statusFormat,
                cellattr : function(rowId, cellValue, rawObject, cm, rdata) {
                    if(cellValue == "OK"){
                        return 'style="background-color:#1ACFE6"';
                    }else{
                        return 'style="background-color:#F05D5D"';
                    }
                }
            },
            {index:'name',name:'op',align:"center",formatter:opFormat}
        ],
        width:900,
        height:400
    });

    function statusFormat(value,options,data){
        if(value == 0){
            return "OK";
        }else{
            return "ERROR";
        }

    }

    function opFormat(value,options,data){
        if(data.status == -1){
            return '<input type="button" value="恢复" onclick="op(\'' + data.name+'\',\'drc\')" />';
        }else{
            return '<input type="button" value="重启" onclick="op(\'' + data.name+'\',\'drs\')" />';
        }
    }

    function op(name,type){
        $.ajax({
            url: 'op',
            type: 'post',
            dataType: 'json',
            data: {"action":type,"name":name},
            success: function(data){
                if(data.success == "true"){

                }else{
                    alert("操作失败，" + data.result);
                }
                $.ligerDialog.closeWaitting();
                console.info(data);
            },
            error: function(xmlResponse){
                alert("操作失败" + xmlResponse.responseText);
                $.ligerDialog.closeWaitting();
            }
        });
    }

</script>
</body>
</html>