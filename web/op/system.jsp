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
<script src="lib/jquery/jquery-1.7.js" type="text/javascript"></script>
<script src="lib/jquery/jquery-dateFormat.js" type="text/javascript"></script>
<script src="lib/calendar/lhgcalendar.min.js" type="text/javascript"></script>
<script>

    $(function ()
    {
        $('#startTime').calendar({format:'yyyy-MM-dd HH:mm:ss'});
        $('#endTime').calendar({format:'yyyy-MM-dd HH:mm:ss'});

        loadProjects();
    })

    function loadProjects(){
        $.ajax({
            url: 'ad',
            data: {"action":'prj'},
            type: 'post',
            dataType: 'json',
            success:function(data){
                $('#pid').empty();
                for(var k in data){
                    console.info(data[k] + ":" + k);
                    $("<option value='"+ data[k]+"'>" + k +"</option>").appendTo('#pid');
                }

                console.info(data);
            }
        });
    }

    function query(){
        var startTime = $("#startTime").val();
        var endTime = $("#endTime").val();
        var nation = $("#nation").val();
        var pid = $("#pid").val();

        if(!startTime || !endTime){
            alert("时间不能为空");
            return;
        }

        if(!nation){
            alert("国家不能为空");
            return;
        }

        if(startTime >= endTime){
            alert("开始时间不能大于结束时间");
            return ;
        }

        console.info(startTime + " : " + endTime + " : " + nation + " : " + pid)

        $("#content").html("");
        $.ajax({
            type: 'POST',
            url:"ad",
            data:'action=hit&startTime=' + startTime + "&endTime=" + endTime + "&nation=" + nation + "&pid=" + pid,
            success: function(msg){
                $("#content").html(msg);
            }
        });
    }

</script>

<body>
    项目名：<select type="text" id="pid" name="pid" style="width: 200px"></select>
    开始时间：<input name="startTime" id="startTime" />
    结束时间：<input name="endTime" id="endTime" />
    Nation: <input name="nation" id="nation"/>

    <input type="button" value="查询" onclick="query()"/> <br/>
    <br/>
    <br/>

    <div id="content" style="height:200px;width: 400px;background: #000000;color: #ffffff" >

    </div>

</body>
</html>