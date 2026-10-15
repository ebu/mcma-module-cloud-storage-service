// Temporary migration bridge. Remove after publishing and adopting the ESM MCMA release.
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const root = path.resolve(__dirname, '..');
const output = path.join(root, 'build/local-mcma');
const json = file => JSON.parse(fs.readFileSync(file, 'utf8'));
function npm(args, cwd) {
    const executable = process.env.npm_execpath;
    const result = executable
        ? spawnSync(process.execPath, [executable, ...args], { cwd, encoding:'utf8', stdio:'inherit' })
        : spawnSync(process.platform === 'win32' ? 'cmd' : 'npm', process.platform === 'win32' ? ['/c','npm',...args] : args, { cwd, encoding:'utf8', stdio:'inherit' });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`npm ${args[0]} failed in ${cwd}`);
}
function packages(directory, groups) {
    return groups.flatMap(group => fs.readdirSync(path.join(directory,group), {withFileTypes:true})
        .filter(e=>e.isDirectory() && fs.existsSync(path.join(directory,group,e.name,'package.json')))
        .map(e=>{const dir=path.join(directory,group,e.name); return {dir,manifest:json(path.join(dir,'package.json'))};}));
}
const deps = manifest => Object.keys({...manifest.dependencies,...manifest.devDependencies,...manifest.peerDependencies}).filter(n=>n.startsWith('@mcma/'));
function prepare(directory = process.env.MCMA_LIBRARIES_ROOT) {
    if (!directory) throw new Error('Set MCMA_LIBRARIES_ROOT to the built ESM mcma-libraries checkout.');
    const available = new Map(packages(path.resolve(directory),['base','aws','azure','google-cloud']).map(p=>[p.manifest.name,p]));
    const selected = new Map();
    function select(name) {
        if(selected.has(name)) return;
        const pkg=available.get(name);
        assert.ok(pkg,`Missing local MCMA package ${name}`);
        assert.equal(pkg.manifest.type,'module');
        assert.ok(fs.existsSync(path.join(pkg.dir,pkg.manifest.main)),`Build ${name} with npm run verify first`);
        selected.set(name,pkg);
        deps(pkg.manifest).forEach(select);
    }
    for(const pkg of packages(root,['libraries','aws','azure','scripts'])) deps(pkg.manifest).forEach(select);
    fs.mkdirSync(output,{recursive:true});
    const index={};
    for(const [name,pkg] of selected) {
        npm(['pack','--ignore-scripts','--pack-destination',output],pkg.dir);
        index[name]={version:pkg.manifest.version,dependencies:deps(pkg.manifest),file:path.join(output,`${name.replace('@','').replace('/','-')}-${pkg.manifest.version}.tgz`)};
    }
    fs.writeFileSync(path.join(output,'index.json'),JSON.stringify(index,null,2));
    console.log(`Prepared ${selected.size} local ESM MCMA tarballs.`);
}
function rewriteManifest(manifest,index) {
    for(const key of ['dependencies','devDependencies','peerDependencies']) {
        for(const name of Object.keys(manifest[key] || {})) {
            if(index[name]) manifest[key][name]=index[name].version;
        }
    }
    return manifest;
}
function stage(file) {
    const index=json(path.join(output,'index.json'));
    fs.writeFileSync(file,JSON.stringify(rewriteManifest(json(file),index),null,4)+'\n');
}
function install(dir,localTarballs=[]) {
    dir=path.resolve(dir);
    const index=json(path.join(output,'index.json'));
    const selected=new Set();
    function select(name) {
        if(selected.has(name)) return;
        assert.ok(index[name],`Missing packed MCMA dependency ${name}`);
        selected.add(name);
        index[name].dependencies.forEach(select);
    }
    const localPackages=new Map(packages(root,['libraries']).map(p=>[p.manifest.name,p]));
    const visited=new Set();
    function inspect(pkgDir) {
        if(visited.has(pkgDir)) return;
        visited.add(pkgDir);
        deps(json(path.join(pkgDir,'package.json'))).forEach(select);
        function scan(sourceDir) {
            if(!fs.existsSync(sourceDir)) return;
            for(const e of fs.readdirSync(sourceDir,{withFileTypes:true})) {
                const file=path.join(sourceDir,e.name);
                if(e.isDirectory()) scan(file);
                else if(e.name.endsWith('.ts')) for(const m of fs.readFileSync(file,'utf8').matchAll(/["'](@local\/[^"']+)["']/g)) {
                    const local=localPackages.get(m[1]);
                    assert.ok(local,`Missing local package ${m[1]}`);
                    inspect(local.dir);
                }
            }
        }
        scan(path.join(pkgDir,'src'));
    }
    inspect(dir);
    const manifestFile=path.join(dir,'package.json');
    const original=fs.readFileSync(manifestFile,'utf8');
    try {
        fs.writeFileSync(manifestFile,JSON.stringify(rewriteManifest(JSON.parse(original),index),null,4)+'\n');
        npm(['install','--no-save','--package-lock=false','--no-audit','--no-fund',...Array.from(selected,name=>index[name].file),...localTarballs],dir);
    } finally {
        fs.writeFileSync(manifestFile,original);
    }
    for(const name of selected) assert.equal(json(path.join(dir,'node_modules',name,'package.json')).type,'module',`Expected ESM ${name}`);
    console.log(`Installed ${selected.size} local ESM MCMA packages into ${path.relative(root,dir)}.`);
}
module.exports={prepare,install,stage};
if(require.main===module) {
    const [command,...args]=process.argv.slice(2);
    if(command==='pack') prepare(args[0]);
    else if(command==='install') install(args[0],args.slice(1));
    else if(command==='stage') stage(args[0]);
    else throw new Error('Expected pack, install, or stage');
}
