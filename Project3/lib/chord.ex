defmodule Chord do
  use GenServer

  defmodule ChordAlg do
    def inrange(id,start,last, max) do
      last = if last < 0 do max-1 else last end
      start = rem(start,max) |>round
      last = rem(last,max) |> round
      if(start<=last) do
          Enum.member?(start..last,id)
      else
          Enum.member?(start..max,id) || Enum.member?(0..last,id)
      end
    end

      def in_fingers(nodes,m,max) do
        Enum.each nodes , fn {id,pid} ->
        table = get_table([],id,nodes,m,max,0)
        GenServer.call(pid,{:in_fingers,table})
        end
    end

    def get_table(list,id,nodes,m,max,power) do
        cond do
        power == m ->
            list
        true->
            next_id = id + :math.pow(2, power)
            next = Chord.Node.get_next_node(next_id, nodes, max)
            list = list ++ [{next,Map.get(nodes,next)}]
            get_table(list,id,nodes,m,max,power+1)
        end
    end

    def finalize(numNodes,numRequests,num_messages\\0,total_hops\\0) do
          receive do
              {:message,_,hops} ->
                  total_hops = total_hops+hops
                  num_messages = num_messages+1
                  if num_messages < numNodes*numRequests do
                      finalize(numNodes,numRequests,num_messages,total_hops)
                  else
                      IO.puts "#{total_hops/(numRequests*numNodes)}"
                      System.halt(0) #done
                  end
          end
      end

      def startmsg(nodes,numRequests,m,max) do
          if numRequests > 0 do
                  Enum.each nodes, fn {id,pid}->
                  rand_node = :rand.uniform(max)
                  {key_temp,_} = Integer.parse(Integer.to_string(rand_node) |> Base.encode16, 16)
                  key = rem(key_temp, max) |> round
                  {:ok,hops} = GenServer.call(pid,{:startmessage,id,key,m,0,max})
                  Process.send(self(),{:message,id,hops},[])
              end
          startmsg(nodes,numRequests-1,m,max)
          end
      end
    end


  defmodule Node do
    use GenServer

    def generate(list,curr,rem,total) do
      nextNode = if rem > 0, do: curr + 2, else: curr + 1
      if total == 0 do
          list
      else
          {:ok,pid} = GenServer.start(__MODULE__,%{:id => curr,
              :node_identifier => Integer.to_string(curr) |> Base.encode16,
              :next => nextNode})
          list = Map.put(list, curr, pid)
          generate(list,nextNode,rem-1,total-1)
      end
    end

    def get_next_node(id,nodes,max) do
        id = rem(id |> round, max) |> round
        cond do
            Map.has_key?(nodes,id) ->
                id
            true ->
                get_next_node(id+1,nodes,max)
        end
    end

    def getprec(id,key,m,table) do
        max =  :math.pow(2,m) |> round
        Enum.find(table,fn{x,_} -> ChordAlg.inrange(x,id+1,key-1,max) end)
    end


    def handle_call({:gettable},_,state) do
        {:reply,state.finger_table,state}
    end

    def handle_call({:getnext},_,state) do
        {:reply,state.next,state}
    end

    def handle_call({:startmessage,id,key,m,hops,max},_,state) do
        next = state.next
        if ChordAlg.inrange(key,id+1,next+0,max) do
            {:reply,{:ok,hops},state}
        else
            table = state.finger_table
            {prec,pid} = getprec(id,key,m,Enum.reverse(table))
            {:reply,GenServer.call(pid,{:startmessage,prec,key,m,hops+1,max}),state}
        end
    end
    def handle_call({:in_fingers,table},_, state) do
        state = Map.put(state,:finger_table, table)
        {:reply, :ok,state}
    end

    def init(args) do
      {:ok,args}
    end

  end

  def init(args) do
    {:ok,args}
  end
end

defmodule Project3 do

  #numNodes, numRequests
  def init([arg1,arg2]) do
    numNodes = String.to_integer(arg1)
    numRequests = String.to_integer(arg2)

    m = numNodes |> :math.log2 |> round

    max =  :math.pow(2,m) |> round
    rem = rem(max,numNodes) |> round
    nodes = Chord.Node.generate(%{},0,rem,numNodes)
    Chord.ChordAlg.in_fingers(nodes,m,max)
    Chord.ChordAlg.startmsg(nodes,numRequests,m,max)
    Chord.ChordAlg.finalize(numNodes,numRequests)
  end

  def start(_,_) do
    [numNodes, numRequests] = System.argv()
    init([numNodes,numRequests])
  end


end
