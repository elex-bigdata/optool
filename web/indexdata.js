var indexdata = 
[
    { text: '广告日志',isexpand:false, children: [
		{url:"ad/adhit.jsp",text:"日志查询"}
	]
    },
    { text: '工具箱', isexpand: false, children: [
		{ url: "op/uid.jsp", text: "UID转换" },
        { url: "op/tools.jsp", text: "小工具" }
	]
    },
    { text: '系统检测', isexpand: false, children: [
        { url: "op/drill.jsp", text: "系统自检" },
        { url: "op/drill.jsp", text: "Drill" },
        { url: "op/dataloader.jsp", text: "DataLoder" },
        { url: "op/internet.jsp", text: "Internet" }
    ]
    }
];


var indexdata2 =
[
    { isexpand: "false", text: "表格", children: [
        { isexpand: "false", text: "可排序", children: [
		    { url: "dotnetdemos/grid/sortable/client.aspx", text: "客户端" },
            { url: "dotnetdemos/grid/sortable/server.aspx", text: "服务器" }
	    ]
        },
        { isexpand: "true", text: "树表格", children: [
		    { url: "dotnetdemos/grid/treegrid/tree.aspx", text: "树表格" }, 
		    { url: "dotnetdemos/grid/treegrid/tree2.aspx", text: "树表格2" }
	    ]
        }
    ]
    }
];
