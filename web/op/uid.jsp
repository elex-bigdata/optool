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

    })


    function query(){
        var uids = $("#uids").val();
        var pid = $("#pid").val();
        var idtype = $("#idtype").val();

        if(!pid){
            alert("项目名称不能为空");
            return;
        }

        if(!uids){
            alert("UID不能为空");
            return;
        }


        $("#content").html("");
        $.ajax({
            type: 'POST',
            url:"op",
            data:'action=tuid&project=' + pid + "&uids=" + uids + "&idtype=" + idtype ,
            success: function(msg){
                $("#content").html(msg);
            }
        });
    }

</script>

<body>
    项目名：<input name="pid" id="pid" /></br>
    id类型：<select id="idtype"  style="width: 160px">
             <option value="hash">哈希UID</option>
             <option value="org">原始ID</option>
           </select></br>
    id列表（逗号分隔）: </br><textarea  name="uids" id="uids" rows="5" cols="80" ></textarea>

    <input type="button" value="查询" onclick="query()"/> <br/>
    <br/>
    <br/>

    <div id="content" style="height:200px;width: 700px;background: #000000;color: #ffffff" >

    </div>

</body>
</html>