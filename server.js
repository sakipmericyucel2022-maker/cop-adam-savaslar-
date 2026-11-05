const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const server = http.createServer((req,res)=>{
  const pathname = decodeURIComponent((req.url||'/').split('?')[0]);
  const file = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const full = path.join(ROOT,file);
  if (!full.startsWith(ROOT) || !fs.existsSync(full) || fs.statSync(full).isDirectory()) {res.writeHead(404);return res.end('Not found');}
  const ext=path.extname(full);res.writeHead(200,{'Content-Type':ext==='.html'?'text/html; charset=utf-8':'application/octet-stream'});fs.createReadStream(full).pipe(res);
});
const wss = new WebSocket.Server({server});
const rooms = new Map(); let waiting = null;
const code=()=>Math.random().toString(36).slice(2,8).toUpperCase();
const send=(ws,obj)=>{if(ws&&ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj));};
wss.on('connection',ws=>{
  ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch{return;}
    if(m.type==='create'){
      let id;do{id=code()}while(rooms.has(id));rooms.set(id,{host:ws,guest:null});ws.room=id;ws.role='host';send(ws,{type:'created',room:id});
    } else if(m.type==='join'){
      const room=rooms.get(String(m.room||'').toUpperCase());if(!room||room.guest){send(ws,{type:'error',message:'Oda bulunamadı veya dolu.'});return;}
      room.guest=ws;ws.room=String(m.room).toUpperCase();ws.role='guest';send(ws,{type:'joined',room:ws.room});send(room.host,{type:'matched',role:'host',room:ws.room});send(ws,{type:'matched',role:'guest',room:ws.room});
    } else if(m.type==='quick'){
      if(waiting&&waiting.readyState===WebSocket.OPEN&&waiting!==ws){const other=waiting;waiting=null;const id=code();rooms.set(id,{host:other,guest:ws});other.room=id;other.role='host';ws.room=id;ws.role='guest';send(other,{type:'matched',role:'host',room:id});send(ws,{type:'matched',role:'guest',room:id});}
      else {waiting=ws;ws.waiting=true;send(ws,{type:'waiting'});}
    } else if(ws.room){const room=rooms.get(ws.room);if(!room)return;const other=ws.role==='host'?room.guest:room.host;if(m.type==='state'||m.type==='ended'||m.type==='spawn')send(other,m);}
  });
  ws.on('close',()=>{if(waiting===ws)waiting=null;if(ws.room){const room=rooms.get(ws.room);if(room){const other=ws.role==='host'?room.guest:room.host;send(other,{type:'error',message:'Rakip bağlantıyı kesti.'});rooms.delete(ws.room);}}});
});
server.listen(PORT,()=>console.log(`Çöp Adam Savaşları online server: http://localhost:${PORT}`));
