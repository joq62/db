%%% -------------------------------------------------------------------
%%% Author  : uabjle
%%% Description : resource discovery accroding to OPT in Action 
%%% This service discovery is adapted to 
%%% Type = application 
%%% Instance ={ip_addr,{IP_addr,Port}}|{erlang_node,{ErlNode}}
%%% 
%%% Created : 10 dec 2012
%%% -------------------------------------------------------------------
-module(db_oam).

-behaviour(gen_server).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

%% --------------------------------------------------------------------

%% External exports
-export([
	 first_node/0,
%	 add_nodes/0,
	 install_first_node/0
	]).

-export([
	 start/0,
	 stop/0,
	 appl_start/1,
	 ping/0
	]).


%% gen_server callbacks



-export([init/1, handle_call/3,handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%-------------------------------------------------------------------
-record(state, {db_callbacks,
		nodes,
		first_node,
		nodes_to_add
	       }).
%-define(DbCallbacks,[application_spec,deployment_info,deployments,host_spec]).

%% ====================================================================
%% External functions
%% ====================================================================
appl_start([])->
    application:start(?MODULE).
first_node()->
    gen_server:call(?MODULE, {first_node},infinity).
install_first_node()->
    gen_server:call(?MODULE, {install_first_node},infinity).
%% call
start()-> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
stop()-> gen_server:call(?MODULE, {stop},infinity).

ping()->
    gen_server:call(?MODULE,{ping},infinity).

%% cast

%% ====================================================================
%% Server functions
%% ====================================================================

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init([]) ->
    application:start(config),
    {ok,DbCallBacks}=application:get_env(db_callbacks),
    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
				 {"DBG, DbCallBacks ",DbCallBacks,node()}]), 
    {ok,Nodes}=application:get_env(nodes),
    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
				 {"DBG, Nodes ",Nodes,node()}]), 

   
    % Install local
    DynamicDbInit=dynamic_db:init(DbCallBacks,[]),
    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
				 {"DBG, DynamicDbInit ",DynamicDbInit,node()}]),
    DynamicDbInitTables=dynamic_db:init_tables(DbCallBacks,node(),node()),
    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
				 {"DBG, DynamicDbInitTables ",DynamicDbInitTables,node()}]),   
    
    [FirstNode|NodesToAdd]=Nodes,
    rpc:cast(node(),nodelog,log,[notice,?MODULE_STRING,?LINE,
				 {"OK, started server  ",?MODULE,node()}]), 
    {ok, #state{db_callbacks=DbCallBacks,
		nodes=Nodes,
		first_node=FirstNode,
		nodes_to_add=NodesToAdd}}.   
  % {ok, #state{},0}.

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_call({install_first_node},_From, State) ->
    FirstNode=State#state.first_node,
    DbCallBacks=State#state.db_callbacks,
    Nodes=[],
    ok=rpc:call(FirstNode,application,set_env,
		[[{db,[{db_callbacks,DbCallBacks},
				 {nodes,Nodes}]}]]),

    ok=rpc:call(FirstNode,application,start,[db]),

    DynamicDbInitTables=dynamic_db:init_tables(DbCallBacks,node(),FirstNode),  
    Reply=DynamicDbInitTables,
    {reply, Reply, State};


handle_call({first_node},_From, State) ->
    Reply=State#state.first_node,
    {reply, Reply, State};

handle_call({add_nodes},_From, State) ->
    NodesToAdd=lists:delete(node(),State#state.nodes),
    
    Reply=State#state.first_node,
    {reply, Reply, State};

handle_call({ping},_From, State) ->
    Reply=pong,
    {reply, Reply, State};

handle_call(Request, From, State) ->
    Reply = {unmatched_signal,?MODULE,Request,From},
    {reply, Reply, State}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast(Msg, State) ->
    io:format("unmatched match cast ~p~n",[{Msg,?MODULE,?LINE}]),
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info(Info, State) ->
    io:format("unmatched match~p~n",[{Info,?MODULE,?LINE}]), 
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------
