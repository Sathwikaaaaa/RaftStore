import { useState, useEffect } from 'react';
import { 
  Database, Activity, Zap, FileText, LayoutDashboard, 
  Settings, Clock, CheckCircle, Server, Crown, AlertTriangle, ShieldCheck
} from 'lucide-react';

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

          </div>
        </main>
      </div>
    </div>
  );
}

export default App;