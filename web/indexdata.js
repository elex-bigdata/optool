var indexdata = 
[
    { text: '广告日志',isexpand:false, children: [
		{url:"ad/adhit.jsp",text:"日志查询"}
	]
    },
    { text: '工具箱', isexpand: false, children: [
		{ url: "demos/filter/filter.htm", text: "转换工具" }
	]
    },
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
