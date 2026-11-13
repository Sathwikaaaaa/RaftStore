import { useState, useEffect } from 'react';
import { 
  Database, Activity, Zap, FileText, LayoutDashboard, 
  Settings, Clock, CheckCircle, Server, Crown, AlertTriangle, ShieldCheck, BarChart2 
} from 'lucide-react';
import { 
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend 
} from 'recharts';

interface NodeStatus {
  node_id: string;
  state: string;
  term: number;
  log_index: number;
  commit_index: number;
  uptime_sec: number;
  isOnline: boolean;
}

const DEFAULT_NODES: NodeStatus[] = [
  { node_id: 'node1', state: 'Unknown', term: 0, log_index: 0, commit_index: 0, uptime_sec: 0, isOnline: false },
  { node_id: 'node2', state: 'Unknown', term: 0, log_index: 0, commit_index: 0, uptime_sec: 0, isOnline: false },
  { node_id: 'node3', state: 'Unknown', term: 0, log_index: 0, commit_index: 0, uptime_sec: 0, isOnline: false },
];

function App() {
  const [time, setTime] = useState(new Date().toLocaleTimeString());
  const [nodes, setNodes] = useState<NodeStatus[]>(DEFAULT_NODES);
  const [chartData, setChartData] = useState<any[]>([]);
  // KV Operations State
  const [kvOp, setKvOp] = useState<'PUT' | 'GET' | 'DELETE'>('PUT');
  const [kvKey, setKvKey] = useState('');
  const [kvValue, setKvValue] = useState('');
  const [kvResult, setKvResult] = useState<any>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  // Fault Injection State
  const [faultEvents, setFaultEvents] = useState<{time: string, event: string, node: string, status: string}[]>([]);
  const [isInjecting, setIsInjecting] = useState(false);

  // Fault Execute Handler
  const handleInjectFault = async (action: string, actionName: string, targetNode: string) => {
    setIsInjecting(true);
    
    // Log the event in the UI table immediately
    const newEvent = {
      time: new Date().toLocaleTimeString(),
      event: actionName,
      node: targetNode.toUpperCase(),
      status: 'Injected'
    };
    setFaultEvents(prev => [newEvent, ...prev].slice(0, 5)); // Keep the latest 5 events

    const portMatch = targetNode.match(/\d+/);
    const port = portMatch ? 6000 + parseInt(portMatch[0]) : 6001;

    try {
      await fetch(`http://localhost:${port}/api/fault`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action })
      });
    } catch (err) {
      console.error("Fault injection failed:", err);
    }
    
    setIsInjecting(false);
  };
  // KV Execute Handler
  const handleExecuteKV = async () => {
    const leader = nodes.find(n => n.state === 'Leader');
    if (!leader) {
      setKvResult({ success: false, error: 'No active leader available.' });
      return;
    }
    
    setIsExecuting(true);
    const portMatch = leader.node_id.match(/\d+/);
    const port = portMatch ? 6000 + parseInt(portMatch[0]) : 6001;

    try {
      const res = await fetch(`http://localhost:${port}/api/kv`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ op: kvOp, key: kvKey, value: kvValue })
      });
      const data = await res.json();
      setKvResult(data);
    } catch (err) {
      setKvResult({ success: false, error: 'Network error or node unreachable.' });
    }
    setIsExecuting(false);
  };

  // Live clock updater
  useEffect(() => {
    const timer = setInterval(() => setTime(new Date().toLocaleTimeString()), 1000);
    return () => clearInterval(timer);
  }, []);

  // Poll Go Backend APIs every second
  useEffect(() => {
    const fetchClusterStatus = async () => {
      const endpoints = [
        'http://localhost:6001/api/status',
        'http://localhost:6002/api/status',
        'http://localhost:6003/api/status'
      ];

      const results = await Promise.allSettled(
        endpoints.map(url => fetch(url).then(res => res.json()))
      );

      const updatedNodes = results.map((result, index) => {
        if (result.status === 'fulfilled' && result.value) {
          return { ...result.value, isOnline: true };
        }
        return { ...DEFAULT_NODES[index], isOnline: false };
      });

      setNodes(updatedNodes);
      
      // Update rolling chart data (keep the last 30 seconds)
      setChartData(prev => {
        const now = new Date();
        const timeStr = `${now.getMinutes()}:${now.getSeconds().toString().padStart(2, '0')}`;
        
        // Simulating throughput and latency for the UI wireframe since we haven't 
        // hooked up the prometheus metric aggregation to the /api/status endpoint yet.
        const activeThroughput = updatedNodes.some(n => n.isOnline) ? Math.floor(Math.random() * 40) + 120 : 0;
        const activeLatency = updatedNodes.some(n => n.isOnline) ? Math.floor(Math.random() * 15) + 30 : 0;

        const newPoint = {
          time: timeStr,
          N1: updatedNodes[0]?.log_index || 0,
          N2: updatedNodes[1]?.log_index || 0,
          N3: updatedNodes[2]?.log_index || 0,
          throughput: activeThroughput,
          latency: activeLatency,
        };
        return [...prev.slice(-29), newPoint]; 
      });
    };

    fetchClusterStatus();
    const interval = setInterval(fetchClusterStatus, 1000);
    return () => clearInterval(interval);
  }, []);

  // Derived KPIs
  const onlineNodes = nodes.filter(n => n.isOnline).length;
  const leaderNode = nodes.find(n => n.state === 'Leader');
  const maxLogIndex = Math.max(...nodes.map(n => n.log_index));
  const maxUptime = Math.max(...nodes.map(n => n.uptime_sec));
  
  const formatUptime = (seconds: number) => {
    const h = Math.floor(seconds / 3600).toString().padStart(2, '0');
    const m = Math.floor((seconds % 3600) / 60).toString().padStart(2, '0');
    const s = (seconds % 60).toString().padStart(2, '0');
    return `${h}:${m}:${s}`;
  };

  return (
    <div className="min-h-screen flex flex-col font-sans">
      
      {/* Top Header */}
      <header className="bg-slate-900 text-white h-14 flex items-center justify-between px-6 shadow-md z-10">
        <div className="flex items-center space-x-3">
          <Database className="w-6 h-6 text-blue-400" />
          <h1 className="text-xl font-bold tracking-wide">RaftStore</h1>
          <span className="text-slate-400 text-sm hidden md:inline ml-4 border-l border-slate-700 pl-4">
            Adaptive and Fault-Tolerant Distributed Key-Value Storage Engine
          </span>
        </div>
        <div className="flex items-center space-x-6">
          <div className={`flex items-center px-3 py-1 rounded-full text-sm font-medium border ${onlineNodes === 3 ? 'bg-green-900/30 text-green-400 border-green-800/50' : 'bg-red-900/30 text-red-400 border-red-800/50'}`}>
            {onlineNodes === 3 ? <CheckCircle className="w-4 h-4 mr-2" /> : <AlertTriangle className="w-4 h-4 mr-2" />}
            {onlineNodes === 3 ? 'Cluster Running' : 'Cluster Degraded'}
          </div>
          <div className="flex items-center text-slate-300 text-sm font-mono">
            <Clock className="w-4 h-4 mr-2" />
            {time}
          </div>
        </div>
      </header>

      {/* Main Layout */}
      <div className="flex flex-1 overflow-hidden">
        
        {/* Sidebar */}
        <aside className="w-56 bg-white border-r border-slate-200 flex flex-col py-4 hidden md:flex">
          <nav className="flex-1 space-y-1 px-3">
            {[
              { name: 'Dashboard', icon: LayoutDashboard, active: true },
              { name: 'Key-Value Operations', icon: Database },
              { name: 'Cluster Status', icon: Activity },
              { name: 'ML Adaptation', icon: Settings },
              { name: 'Fault Injection', icon: Zap },
              { name: 'Logs', icon: FileText },
            ].map((item) => (
              <a key={item.name} href="#" className={`flex items-center px-3 py-2.5 text-sm font-medium rounded-lg ${item.active ? 'bg-blue-50 text-blue-700' : 'text-slate-600 hover:bg-slate-50 hover:text-slate-900'}`}>
                <item.icon className={`w-5 h-5 mr-3 ${item.active ? 'text-blue-600' : 'text-slate-400'}`} />
                {item.name}
              </a>
            ))}
          </nav>
        </aside>

        {/* Main Content Area */}
        <main className="flex-1 overflow-y-auto p-6">
          <div className="max-w-7xl mx-auto space-y-6">
            
            {/* KPI Strip */}
            <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
              <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-4 flex items-center space-x-4">
                <div className="p-3 bg-blue-50 rounded-lg"><Server className="w-6 h-6 text-blue-600" /></div>
                <div><p className="text-sm text-slate-500 font-medium">Total Nodes</p><p className="text-2xl font-bold text-slate-800">{onlineNodes} / 3</p></div>
              </div>
              <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-4 flex items-center space-x-4">
                <div className="p-3 bg-green-50 rounded-lg"><Crown className="w-6 h-6 text-green-600" /></div>
                <div><p className="text-sm text-slate-500 font-medium">Leader Node</p><p className="text-2xl font-bold text-slate-800">{leaderNode ? leaderNode.node_id.toUpperCase() : 'None'}</p></div>
              </div>
              <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-4 flex items-center space-x-4">
                <div className="p-3 bg-purple-50 rounded-lg"><FileText className="w-6 h-6 text-purple-600" /></div>
                <div><p className="text-sm text-slate-500 font-medium">Total Log Entries</p><p className="text-2xl font-bold text-slate-800">{maxLogIndex}</p></div>
              </div>
              <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-4 flex items-center space-x-4">
                <div className="p-3 bg-yellow-50 rounded-lg"><Zap className="w-6 h-6 text-yellow-600" /></div>
                <div><p className="text-sm text-slate-500 font-medium">Cluster Uptime</p><p className="text-xl font-bold text-slate-800 font-mono">{formatUptime(maxUptime)}</p></div>
              </div>
              <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-4 flex items-center space-x-4">
                <div className="p-3 bg-red-50 rounded-lg"><ShieldCheck className="w-6 h-6 text-red-600" /></div>
                <div><p className="text-sm text-slate-500 font-medium">ML Model Status</p><p className="text-lg font-bold text-green-600">Active</p></div>
              </div>
            </div>

            {/* Topology Section */}
            <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
              <h2 className="text-lg font-semibold text-slate-800 mb-4 flex items-center">
                <Activity className="w-5 h-5 mr-2 text-blue-500" /> Cluster Nodes
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {nodes.map(node => (
                  <div key={node.node_id} className={`relative flex flex-col items-center p-6 rounded-xl border-2 transition-all ${node.state === 'Leader' ? 'bg-green-50 border-green-500 shadow-sm' : node.isOnline ? 'bg-white border-slate-200' : 'bg-red-50 border-red-300 opacity-75'}`}>
                    {node.state === 'Leader' && <Crown className="absolute -top-3 text-yellow-500 w-8 h-8 drop-shadow-md bg-white rounded-full p-1 border border-slate-100" />}
                    <Server className={`w-12 h-12 mb-3 ${node.state === 'Leader' ? 'text-green-600' : node.isOnline ? 'text-blue-500' : 'text-red-400'}`} />
                    <h3 className="text-lg font-bold text-slate-800 uppercase tracking-wide">{node.node_id}</h3>
                    <span className={`px-3 py-1 mt-2 rounded-full text-xs font-bold uppercase tracking-wider ${node.state === 'Leader' ? 'bg-green-200 text-green-800' : node.isOnline ? 'bg-slate-100 text-slate-600' : 'bg-red-200 text-red-800'}`}>
                      {node.isOnline ? node.state : 'Offline'}
                    </span>
                    <div className="w-full mt-5 space-y-2 border-t border-slate-200/60 pt-4">
                      <div className="flex justify-between text-sm"><span className="text-slate-500">Term:</span><span className="font-mono font-semibold text-slate-700">{node.term}</span></div>
                      <div className="flex justify-between text-sm"><span className="text-slate-500">Log Index:</span><span className="font-mono font-semibold text-slate-700">{node.log_index}</span></div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
{/* Key-Value Operations Section */}
            <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
              <h2 className="text-lg font-semibold text-slate-800 mb-4 flex items-center">
                <Database className="w-5 h-5 mr-2 text-blue-500" /> Key-Value Operations
              </h2>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                
                {/* Left Column: Form */}
                <div className="space-y-4">
                  <div className="flex border border-slate-200 rounded-lg overflow-hidden font-medium text-sm">
                    {['PUT', 'GET', 'DELETE'].map(op => (
                      <button 
                        key={op}
                        onClick={() => setKvOp(op as any)}
                        className={`flex-1 py-2 text-center transition-colors ${kvOp === op ? 'bg-blue-600 text-white' : 'bg-slate-50 text-slate-600 hover:bg-slate-100'}`}
                      >
                        {op}
                      </button>
                    ))}
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Key</label>
                    <input 
                      type="text" 
                      placeholder="Enter key (e.g., user:1)" 
                      value={kvKey}
                      onChange={e => setKvKey(e.target.value)}
                      className="w-full border border-slate-300 rounded-lg px-3 py-2 outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                    />
                  </div>

                  {kvOp === 'PUT' && (
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Value</label>
                      <input 
                        type="text" 
                        placeholder='Enter value (e.g., {"name":"Kanishka"})' 
                        value={kvValue}
                        onChange={e => setKvValue(e.target.value)}
                        className="w-full border border-slate-300 rounded-lg px-3 py-2 outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                      />
                    </div>
                  )}

                  <button 
                    onClick={handleExecuteKV}
                    disabled={isExecuting || !kvKey || (kvOp === 'PUT' && !kvValue)}
                    className="w-full bg-blue-600 hover:bg-blue-700 text-white font-medium py-2 rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {isExecuting ? 'Executing...' : 'Execute'}
                  </button>
                </div>

                {/* Right Column: Result Card */}
                <div className="bg-slate-50 border border-slate-200 rounded-xl p-5 flex flex-col">
                  <h3 className="text-sm font-semibold text-slate-600 mb-3 border-b border-slate-200 pb-2">Operation Result</h3>
                  
                  {!kvResult ? (
                    <div className="flex-1 flex items-center justify-center text-slate-400 text-sm">
                      Execute an operation to see results
                    </div>
                  ) : kvResult.success ? (
                    <div className="space-y-3 text-sm">
                      <div className="flex items-center text-green-600 font-bold mb-2">
                        <CheckCircle className="w-5 h-5 mr-2" /> {kvResult.op} Successful
                      </div>
                      <div className="grid grid-cols-3 gap-2">
                        <span className="text-slate-500">Key:</span>
                        <span className="col-span-2 font-mono text-slate-800">{kvResult.key}</span>
                        
                        {kvResult.op === 'PUT' && (
                          <>
                            <span className="text-slate-500">Value:</span>
                            <span className="col-span-2 font-mono text-slate-800 truncate">{kvResult.value}</span>
                          </>
                        )}
                        
                        <span className="text-slate-500">Node:</span>
                        <span className="col-span-2 font-semibold text-blue-600">Leader ({kvResult.node.toUpperCase()})</span>
                        
                        <span className="text-slate-500">Log Index:</span>
                        <span className="col-span-2 font-mono text-slate-800">{kvResult.log_index}</span>
                        
                        <span className="text-slate-500">Latency:</span>
                        <span className="col-span-2 font-mono text-slate-800">{kvResult.latency_ms} ms</span>
                      </div>
                    </div>
                  ) : (
                    <div className="space-y-2 text-sm text-red-600">
                      <div className="flex items-center font-bold mb-2">
                        <AlertTriangle className="w-5 h-5 mr-2" /> Operation Failed
                      </div>
                      <p>{kvResult.error}</p>
                    </div>
                  )}
                </div>
              </div>
            </div>
            {/* Live Charts Section */}
            <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
              
              {/* Log Replication Status */}
              <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
                <h2 className="text-lg font-semibold text-slate-800 mb-4 flex items-center">
                  <FileText className="w-5 h-5 mr-2 text-blue-500" /> Log Replication Status
                </h2>
                <div className="h-64">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData} margin={{ top: 5, right: 20, bottom: 5, left: -20 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                      <XAxis dataKey="time" tick={{fontSize: 12}} stroke="#94a3b8" />
                      <YAxis tick={{fontSize: 12}} stroke="#94a3b8" domain={['dataMin - 1', 'dataMax + 1']} allowDecimals={false} />
                      <Tooltip contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }} />
                      <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '10px' }} />
                      <Line type="stepAfter" dataKey="N1" stroke="#3b82f6" strokeWidth={2} dot={false} name="Node 1" />
                      <Line type="stepAfter" dataKey="N2" stroke="#22c55e" strokeWidth={2} dot={false} name="Node 2" />
                      <Line type="stepAfter" dataKey="N3" stroke="#eab308" strokeWidth={2} dot={false} name="Node 3" />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Performance Metrics */}
              <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
                <h2 className="text-lg font-semibold text-slate-800 mb-4 flex items-center">
                  <BarChart2 className="w-5 h-5 mr-2 text-blue-500" /> Performance Metrics (Live)
                </h2>
                <div className="h-64 flex flex-col space-y-4">
                  
                  {/* Throughput Sparkline */}
                  <div className="flex-1 bg-slate-50 border border-slate-100 rounded-lg p-3">
                    <div className="flex justify-between items-center mb-1">
                      <span className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Throughput</span>
                      <span className="text-sm font-bold text-sky-600">{chartData.length > 0 ? chartData[chartData.length - 1].throughput : 0} Ops/sec</span>
                    </div>
                    <div className="h-20">
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={chartData}>
                          <YAxis hide domain={['dataMin - 20', 'dataMax + 20']} />
                          <Tooltip contentStyle={{ fontSize: '12px', borderRadius: '4px' }} />
                          <Line type="monotone" dataKey="throughput" stroke="#0ea5e9" strokeWidth={2} dot={false} />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </div>

                  {/* Latency Sparkline */}
                  <div className="flex-1 bg-slate-50 border border-slate-100 rounded-lg p-3">
                    <div className="flex justify-between items-center mb-1">
                      <span className="text-xs font-semibold text-slate-500 uppercase tracking-wider">P95 Latency</span>
                      <span className="text-sm font-bold text-violet-600">{chartData.length > 0 ? chartData[chartData.length - 1].latency : 0} ms</span>
                    </div>
                    <div className="h-20">
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={chartData}>
                          <YAxis hide domain={['dataMin - 10', 'dataMax + 10']} />
                          <Tooltip contentStyle={{ fontSize: '12px', borderRadius: '4px' }} />
                          <Line type="monotone" dataKey="latency" stroke="#8b5cf6" strokeWidth={2} dot={false} />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </div>

                </div>
              </div>

            </div>
            {/* ML & Fault Injection Section */}
            <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
              
              {/* ML Adaptation Panel */}
              <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
                <h2 className="text-lg font-semibold text-slate-800 mb-4 flex items-center">
                  <Settings className="w-5 h-5 mr-2 text-blue-500" /> ML Adaptation (XGBoost)
                </h2>
                
                <div className="grid grid-cols-2 gap-6">
                  {/* Left: Predicted Parameters */}
                  <div>
                    <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3 bg-slate-100 p-2 rounded">Current Parameters (Predicted)</h3>
                    <div className="space-y-3 text-sm">
                      <div className="flex justify-between border-b border-slate-100 pb-1">
                        <span className="text-slate-600 font-medium">Batch Size</span>
                        <span className="font-mono text-slate-800 font-bold">64</span>
                      </div>
                      <div className="flex justify-between border-b border-slate-100 pb-1">
                        <span className="text-slate-600 font-medium">Pipeline Depth</span>
                        <span className="font-mono text-slate-800 font-bold">4</span>
                      </div>
                      <div className="flex justify-between border-b border-slate-100 pb-1">
                        <span className="text-slate-600 font-medium">Replication Mode</span>
                        <span className="font-mono text-slate-800">Full Copy</span>
                      </div>
                      <div className="flex justify-between border-b border-slate-100 pb-1">
                        <span className="text-slate-600 font-medium">Follower Set</span>
                        <span className="font-mono text-slate-800">N1, N2, N3</span>
                      </div>
                    </div>
                  </div>

                  {/* Right: Live Features */}
                  <div>
                    <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3 bg-slate-100 p-2 rounded">Input Features (Live)</h3>
                    <div className="space-y-3 text-sm">
                      <div className="flex justify-between border-b border-slate-100 pb-1">
                        <span className="text-slate-600 font-medium">Request Rate</span>
                        <span className="font-mono text-slate-800">{chartData.length > 0 ? chartData[chartData.length - 1].throughput : 0} ops/sec</span>
                      </div>
                      <div className="flex justify-between border-b border-slate-100 pb-1">
                        <span className="text-slate-600 font-medium">Avg. Latency</span>
                        <span className="font-mono text-slate-800">{chartData.length > 0 ? chartData[chartData.length - 1].latency : 0} ms</span>
                      </div>
                      <div className="flex justify-between border-b border-slate-100 pb-1">
                        <span className="text-slate-600 font-medium">Follower Lag</span>
                        <span className="font-mono text-slate-800">0 entries</span>
                      </div>
                      <div className="flex justify-between border-b border-slate-100 pb-1">
                        <span className="text-slate-600 font-medium">CPU Usage</span>
                        <span className="font-mono text-slate-800">45%</span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              {/* Fault Injection Panel */}
              <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-6 flex flex-col">
                <h2 className="text-lg font-semibold text-slate-800 mb-4 flex items-center">
                  <Zap className="w-5 h-5 mr-2 text-blue-500" /> Fault Injection & Recovery
                </h2>
                
                {/* Chaos Buttons */}
                <div className="grid grid-cols-4 gap-2 mb-6">
                  <button 
                    onClick={() => handleInjectFault('crash', 'Node Crash', leaderNode ? leaderNode.node_id : 'node1')}
                    disabled={isInjecting || !leaderNode}
                    className="text-xs font-bold py-2 px-1 rounded bg-red-50 text-red-600 border border-red-200 hover:bg-red-100 transition-colors disabled:opacity-50"
                  >
                    Crash Leader
                  </button>
                  <button 
                    onClick={() => handleInjectFault('partition', 'Network Partition', 'node3')}
                    disabled={isInjecting || !onlineNodes}
                    className="text-xs font-bold py-2 px-1 rounded bg-orange-50 text-orange-600 border border-orange-200 hover:bg-orange-100 transition-colors disabled:opacity-50"
                  >
                    Partition N3
                  </button>
                  <button 
                    disabled
                    className="text-xs font-bold py-2 px-1 rounded bg-blue-50 text-blue-600 border border-blue-200 opacity-50 cursor-not-allowed"
                  >
                    Add Latency
                  </button>
                  <button 
                    disabled
                    className="text-xs font-bold py-2 px-1 rounded bg-purple-50 text-purple-600 border border-purple-200 opacity-50 cursor-not-allowed"
                  >
                    Packet Loss
                  </button>
                </div>

                {/* Event Log Table */}
                <div className="flex-1 border border-slate-200 rounded-lg overflow-hidden">
                  <table className="w-full text-sm text-left">
                    <thead className="bg-slate-50 text-xs uppercase text-slate-500 font-semibold border-b border-slate-200">
                      <tr>
                        <th className="px-3 py-2">Time</th>
                        <th className="px-3 py-2">Event</th>
                        <th className="px-3 py-2">Node</th>
                        <th className="px-3 py-2">Status</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100">
                      {faultEvents.length === 0 ? (
                        <tr>
                          <td colSpan={4} className="px-3 py-6 text-center text-slate-400 italic">No recent fault events</td>
                        </tr>
                      ) : (
                        faultEvents.map((event, idx) => (
                          <tr key={idx} className="bg-white">
                            <td className="px-3 py-2 font-mono text-slate-500 whitespace-nowrap">{event.time}</td>
                            <td className="px-3 py-2 font-medium text-slate-700">{event.event}</td>
                            <td className="px-3 py-2 font-mono font-bold text-slate-800">{event.node}</td>
                            <td className="px-3 py-2">
                              <span className="text-xs font-bold text-orange-600 bg-orange-50 px-2 py-1 rounded-full">{event.status}</span>
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

            </div>
          </div>
        </main>
      </div>
    </div>
  );
}

export default App;