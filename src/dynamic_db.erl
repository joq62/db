%%% -------------------------------------------------------------------
%%% Author  : uabjle
%%% Description : dbase using dets 
%%% 
%%% Created : 10 dec 2012
%%% -------------------------------------------------------------------
-module(dynamic_db).    
    
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

%%---------------------------------------------------------------------
%% Records for test
%%

%% --------------------------------------------------------------------
%-compile(export_all).

-export([
	 init/2,
	 init_tables/3
	 ]).


-define(StorageType,ram_copies).
-define(WAIT_FOR_TABLES,2*5000).

%% ====================================================================
%% External functions
%% ====================================================================
%% --------------------------------------------------------------------
%% Function:start/0 
%% Description: Initiate the eunit tests, set upp needed processes etc
%% Returns: non
%% --------------------------------------------------------------------
init_tables(DbCallbacks,Source,Dest)->
    InitTables=[{CallBack,CallBack:init_table(Source,Dest)}||CallBack<-DbCallbacks],
    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
				 {"DBG: CreateTables   ",?MODULE,node()}]),  
    InitTables.

%% --------------------------------------------------------------------
%% Function:start/0 
%% Description: Initiate the eunit tests, set upp needed processes etc
%% Returns: non
%% --------------------------------------------------------------------
init(DbCallbacks,Nodes)->
    mnesia:stop(),
    mnesia:delete_schema([node()]),
    mnesia:start(),
    start(DbCallbacks,Nodes).

%% --------------------------------------------------------------------
%% Function:start/0 
%% Description: Initiate the eunit tests, set upp needed processes etc
%% Returns: non
%% --------------------------------------------------------------------
start(DbCallbacks,[])->
    % Add unique code to create the specific tables
 %% Create tables on TestNode
    CreateTables=[{CallBack,CallBack:create_table()}||CallBack<-DbCallbacks],
    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
				 {"DBG: CreateTables   ",?MODULE,node()}]), 
    CreateTables;

start(DbCallbacks,Nodes) ->
    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
				 {"DBG: Nodes   ",Nodes,node()}]), 
    add_extra_nodes(DbCallbacks,Nodes).


%% --------------------------------------------------------------------
%% Function:start/0 
%% Description: Initiate the eunit tests, set upp needed processes etc
%% Returns: non
%% --------------------------------------------------------------------
add_extra_nodes(DbCallbacks,[Node|T])->
    case mnesia:change_config(extra_db_nodes,[Node]) of
	{ok,[Node]}->
	    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
					 {"DBG: add_extra_nodes Node at node()  ",Node,?MODULE,node()}]), 
 
	    AddSchema=mnesia:add_table_copy(schema,node(),?StorageType),
	    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
					 {"DBG: add_extra_nodes AddSchema  ",AddSchema,?MODULE,node()}]), 
	    TablesFromNode=rpc:call(Node,mnesia,system_info,[tables]),
	    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
					 {"DBG: TablesFromNode  ",TablesFromNode}]), 
	    AddTableCopies=[{Table,mnesia:add_table_copy(Table,node(),?StorageType)}||Table<-TablesFromNode,
										      Table/=schema],
	    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
				 {"DBG: AddTableCopies  ",AddTableCopies,?MODULE,node()}]), 
	    Tables=mnesia:system_info(tables),
	    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
					 {"DBG: add_extra_nodes Tables  ",Tables,?MODULE,node()}]),
	    WaitForTables=mnesia:wait_for_tables(Tables,?WAIT_FOR_TABLES),
	    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
					 {"DBG: add_extra_nodes WaitForTables  ",WaitForTables,?MODULE,node()}]);
	Reason->
	    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
					 {"DBG: Didnt connect to Node Reason  ",Reason,?MODULE,node()}]),
	    add_extra_nodes(DbCallbacks,T)
    end.


