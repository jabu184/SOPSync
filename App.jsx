import { useState, useEffect } from 'react';
import io from 'socket.io-client';

// Connect to the Node.js backend
const socket = io('http://localhost:3001');

const MACHINES = ['Linac 1', 'Linac 2', 'Linac 3', 'CT Simulator'];

function App() {
  const [logs, setLogs] = useState([]);
  const [selectedMachine, setSelectedMachine] = useState(MACHINES[0]);
  const [currentStatuses, setCurrentStatuses] = useState({});
  const [faultNote, setFaultNote] = useState('');
  const [faultStatus, setFaultStatus] = useState('Breakdown');

  useEffect(() => {
    // Load initial history when opening the app
    socket.on('initial_data', (data) => {
      setLogs(data);
    });

    socket.on('latest_statuses', (data) => {
      setCurrentStatuses(data);
    });

    // Listen for live updates from anyone else using the app
    socket.on('status_updated', (newRecord) => {
      setLogs((prevLogs) => [newRecord, ...prevLogs]);
      setCurrentStatuses((prev) => ({ ...prev, [newRecord.machine_name]: newRecord.status }));
    });

    return () => {
      socket.off('initial_data');
      socket.off('latest_statuses');
      socket.off('status_updated');
    };
  }, []);

  const updateStatus = (status, fault_note = null) => {
    socket.emit('update_status', { machine_name: selectedMachine, status, fault_note });
  };

  const handleLogFault = () => {
    if (!faultNote.trim()) return;
    updateStatus(faultStatus, faultNote);
    setFaultNote(''); // Clear input after sending
  };

  const getStatusColor = (status) => {
    switch(status) {
      case 'Switched On': return '#3b82f6'; // Professional Blue
      case 'Clinical': return '#22c55e'; // Success Green
      case 'Service/QA': return '#eab308'; // Warning Yellow
      case 'Breakdown': return '#ef4444'; // Alert Red
      case 'Off': return '#6b7280'; // Neutral Gray
      default: return '#6b7280'; // Neutral Gray
    }
  };

  return (
    <div style={{ fontFamily: 'system-ui, sans-serif', maxWidth: '800px', margin: '0 auto', padding: '2rem', color: '#333' }}>
      <h1 style={{ borderBottom: '2px solid #eee', paddingBottom: '10px' }}>Radiotherapy Status Dashboard</h1>
      
      {/* Summary Board for All Machines */}
      <div style={{ display: 'flex', gap: '10px', marginBottom: '20px', flexWrap: 'wrap' }}>
        {MACHINES.map(machine => {
           const status = currentStatuses[machine] || 'Unknown';
           const isSelected = selectedMachine === machine;
           return (
             <div 
               key={machine}
               onClick={() => setSelectedMachine(machine)}
               style={{ 
                 flex: 1, minWidth: '140px', padding: '15px', borderRadius: '8px', 
                 backgroundColor: isSelected ? '#e5e7eb' : '#f9fafb',
                 borderTop: `5px solid ${getStatusColor(status)}`,
                 cursor: 'pointer', textAlign: 'center',
                 boxShadow: isSelected ? '0 0 0 2px #9ca3af' : '0 2px 4px rgba(0,0,0,0.05)',
                 transition: 'all 0.2s'
               }}>
               <h3 style={{ margin: '0 0 10px 0', fontSize: '1.1rem' }}>{machine}</h3>
               <span style={{ 
                 padding: '4px 10px', borderRadius: '12px', backgroundColor: getStatusColor(status), 
                 color: 'white', fontSize: '0.85em', fontWeight: 'bold' 
               }}>
                 {status}
               </span>
             </div>
           )
        })}
      </div>

      <div style={{ backgroundColor: '#f9fafb', padding: '20px', borderRadius: '8px', marginBottom: '2rem', marginTop: '2rem' }}>
        <h2 style={{ marginTop: 0, fontSize: '1.2rem' }}>Update Status: {selectedMachine}</h2>
        <div style={{ display: 'flex', gap: '1rem' }}>
          {['Switched On', 'Clinical', 'Service/QA', 'Breakdown', 'Off'].map((status) => {
            const isCurrent = currentStatuses[selectedMachine] === status;
            return (
              <button 
                key={status}
                onClick={() => updateStatus(status)} 
                disabled={isCurrent}
                style={{ 
                  padding: '10px 20px', backgroundColor: getStatusColor(status), color: 'white', 
                  border: 'none', borderRadius: '6px', cursor: isCurrent ? 'not-allowed' : 'pointer', 
                  fontWeight: 'bold', flex: 1, opacity: isCurrent ? 0.5 : 1
                }}>
                {status}
              </button>
            );
          })}
        </div>
      </div>

      {/* Log a Fault Section */}
      <div style={{ backgroundColor: '#fff1f2', padding: '20px', borderRadius: '8px', marginBottom: '2rem', border: '1px solid #fecdd3' }}>
        <h3 style={{ marginTop: 0, color: '#be123c', fontSize: '1.1rem' }}>Log a Fault on {selectedMachine}</h3>
        <div style={{ display: 'flex', gap: '10px', alignItems: 'center', flexWrap: 'wrap' }}>
          <input 
            type="text" 
            value={faultNote} 
            onChange={(e) => setFaultNote(e.target.value)} 
            placeholder="Describe the fault..."
            style={{ flex: 2, padding: '10px', borderRadius: '6px', border: '1px solid #fda4af', minWidth: '200px' }}
          />
          <select 
            value={faultStatus}
            onChange={(e) => setFaultStatus(e.target.value)}
            style={{ flex: 1, padding: '10px', borderRadius: '6px', border: '1px solid #fda4af', minWidth: '150px' }}
          >
            <option value="Breakdown">Set Status: Breakdown</option>
            <option value="Service/QA">Set Status: Service/QA</option>
            <option value="Off">Set Status: Off</option>
          </select>
          <button 
            onClick={handleLogFault}
            disabled={!faultNote.trim()}
            style={{ padding: '10px 20px', backgroundColor: '#e11d48', color: 'white', border: 'none', borderRadius: '6px', cursor: faultNote.trim() ? 'pointer' : 'not-allowed', fontWeight: 'bold', opacity: faultNote.trim() ? 1 : 0.5 }}
          >
            Submit Fault
          </button>
        </div>
      </div>

      <h2>{selectedMachine} Activity Log</h2>
      <div style={{ border: '1px solid #e5e7eb', borderRadius: '8px', overflow: 'hidden', backgroundColor: 'white' }}>
        {logs.filter(log => log.machine_name === selectedMachine).map((log) => (
          <div key={log.id} style={{ display: 'flex', justifyContent: 'space-between', padding: '1rem', borderBottom: '1px solid #e5e7eb', alignItems: 'center' }}>
            <div>
              <strong style={{ fontSize: '1.1rem' }}>{log.machine_name}</strong>
              {log.fault_note && (
                <div style={{ color: '#be123c', fontWeight: '500', marginTop: '4px', fontSize: '0.95em' }}>
                  ⚠️ Fault Logged: {log.fault_note}
                </div>
              )}
              <div style={{ fontSize: '0.85em', color: '#6b7280', marginTop: '4px' }}>{new Date(log.timestamp).toLocaleString()}</div>
            </div>
            <span style={{ padding: '6px 12px', borderRadius: '20px', backgroundColor: getStatusColor(log.status), color: 'white', fontSize: '0.9em', fontWeight: '500' }}>
              {log.status}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

export default App;