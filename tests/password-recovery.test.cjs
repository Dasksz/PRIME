const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
function setup(error=null){
 const nodes=new Map(),calls=[];
 const el=id=>{if(!nodes.has(id)){const classes=new Set();nodes.set(id,{value:'',textContent:'',style:{},handlers:{},disabled:false,classList:{add:x=>classes.add(x),remove:x=>classes.delete(x),contains:x=>classes.has(x),toggle(x,on){on?classes.add(x):classes.delete(x)}},addEventListener(type,fn){this.handlers[type]=fn},querySelector(){return el(id+'-button')},reset(){this.wasReset=true}})}return nodes.get(id)};
 const context=vm.createContext({window:{location:{origin:'https://dasksz.github.io',pathname:'/PRIME/'}},document:{getElementById:el}});
 vm.runInContext(fs.readFileSync('password-recovery.js','utf8'),context);
 const client={from(){throw Error('Do not read profiles')},auth:{
 resetPasswordForEmail:async(...args)=>{calls.push(args);return {error}},
 signInWithPassword:async(args)=>{calls.push(args);return {error}},
 updateUser:async(args)=>{calls.push(args);return {error}}
 }};
 const controller=context.window.setupPrimePasswordRecovery(client,async()=>calls.push('updated'));
 const submit=id=>el(id).handlers.submit({preventDefault(){}});
 return {el,calls,controller,submit};
}
test('signed-out recovery sends to the entered email with the PRIME project path',async()=>{
 const s=setup();s.el('recovery-email').value=' person@example.com ';
 await s.submit('password-forgot-form');
 assert.equal(s.calls[0][0],'person@example.com');
 assert.equal(s.calls[0][1].redirectTo,'https://dasksz.github.io/PRIME/');
 assert.equal(s.el('password-forgot-form-button').disabled,false);
 assert.match(s.el('auth-message').textContent,/Se este e-mail estiver cadastrado/);
});
test('SMTP errors keep recovery available for retry',async()=>{
 const s=setup({message:'SMTP failure'});await s.submit('password-forgot-form');
 assert.equal(s.el('auth-message').style.color,'#b91c1c');
 assert.equal(s.el('password-forgot-form-button').disabled,false);
});
test('verified recovery stays on the new password form until a successful update',async()=>{
 const s=setup();assert.equal(s.controller.handleAuth('PASSWORD_RECOVERY'),true);
 assert.equal(s.el('password-reset-view').classList.contains('hidden'),false);
 assert.equal(s.el('content-wrapper').classList.contains('hidden'),true);
 assert.equal(s.controller.handleAuth('TOKEN_REFRESHED'),true);
 s.el('new-password').value=s.el('confirm-password').value='Example-Password1!';
 await s.submit('password-reset-form');assert.equal(s.calls[0].password,'Example-Password1!');
 assert.equal(s.controller.isRecovering(),false);assert.equal(s.calls.at(-1),'updated');
});
test('invalid or rejected passwords do not finish recovery',async()=>{
 const s=setup({message:'Password rejected'});s.controller.handleAuth('PASSWORD_RECOVERY');
 s.el('new-password').value='Example-Password1!';s.el('confirm-password').value='different';
 await s.submit('password-reset-form');assert.equal(s.calls.length,0);
 s.el('confirm-password').value=s.el('new-password').value;await s.submit('password-reset-form');
 assert.equal(s.controller.isRecovering(),true);assert.equal(s.calls.includes('updated'),false);
});
test('email login supports the password set by recovery',async()=>{
 const s=setup();s.el('signin-email').value=' person@example.com ';s.el('signin-password').value='Example-Password1!';
 await s.submit('password-signin-form');assert.equal(s.calls[0].email,'person@example.com');
 assert.equal(s.calls[0].password,'Example-Password1!');
});
