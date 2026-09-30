"""Exercise the opt-in Docs sample with fake provider HTTP and real SQL/HTTP/process reads."""

import json

from sqlalchemy import text

from airweave.domains.entities.canonical.tests.test_resume_probe import subprocess_script

SCRIPT = r'''
import asyncio,json,os,sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import MagicMock
sys.path.insert(0, 'tests/live')
import canonical_capture as harness
import workspace_document
import httpx
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
from provider_sample import verify_rest_identity
from airweave.platform.sources.google_drive import GoogleDriveSource
from airweave.platform.configs.config import GoogleDriveConfig
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.storage.paths import StoragePaths

document = {'documentId':'doc','title':'Fixture','tabs':[
    {'tabProperties':{'tabId':'root'},'documentTab':{'body':{'content':[]}},
     'childTabs':[{'tabProperties':{'tabId':'child'},'documentTab':{'body':{'content':[]}}}]}]}
metadata = {'id':'doc','name':'Fixture','mimeType':'application/vnd.google-apps.document',
            'version':'7'}
seen=[]
def response(request):
    assert request.method=='GET'
    seen.append(request.url.path)
    if request.url.path.endswith('/about'):
        return httpx.Response(200,json={'user':{'emailAddress':'synthetic@example.test'}})
    if request.url.path=='/drive/v3/files':
        return httpx.Response(200,json={'files':[metadata]})
    if request.url.path.endswith('/export'):
        return httpx.Response(200,content=b'synthetic-docx')
    if request.url.host=='docs.googleapis.com':
        return httpx.Response(200,json=document)
    assert request.url.path=='/drive/v3/files/doc'
    return httpx.Response(200,json={'version':'7'})

@asynccontextmanager
async def source(name,account,email,key,fence,**options):
    async with httpx.AsyncClient(transport=httpx.MockTransport(response),
            event_hooks={'request':[options['request_hook']]}) as client:
        connector=await GoogleDriveSource.create(auth=StaticTokenProvider('synthetic'),
            logger=MagicMock(),http_client=client,config=GoogleDriveConfig())
        await verify_rest_identity(name,connector,email)
        yield connector,'provider_email'

async def main():
    workspace_document.rest_source=source
    os.environ['LIVE_WORKSPACE_DOCUMENT']='1'
    root=Path(os.environ['TEST_ROOT']);root.chmod(0o700)
    StoragePaths.TEMP_PROCESSING=str(root/'temp')
    engine=create_async_engine(os.environ['CANONICAL_TEST_DATABASE_URL'],
        connect_args={'server_settings':{'search_path':os.environ['TEST_SCHEMA']}})
    try:
        result=await harness.verify('google_drive','synthetic','synthetic@example.test',
            'synthetic',async_sessionmaker(engine,expire_on_commit=False),engine,root)
        assert len(seen)==5
        print(json.dumps(result))
    finally:
        await engine.dispose()
asyncio.run(main())
'''


async def test_workspace_sample_survives_sql_reopen_and_real_http_reader(database, tmp_path):
    async with database() as db:
        schema = await db.scalar(text("select current_schema()"))
    code, output, error = await subprocess_script(
        SCRIPT, {"TEST_ROOT": str(tmp_path), "TEST_SCHEMA": schema}
    )
    assert code == 0, output + error
    result = json.loads(output.splitlines()[-1])
    assert result["records"] == result["journal_changes"] == 1
    assert result["blobs"] == 3 and result["blob_sha_verified"]
    assert result["replay_new_changes"] == 0 and result["fresh_connections_readback"]
    assert not result["full_scope_completed"] and not result["checkpoint_saved"]
    assert result["almanac_reader"] is None
    assert result["document_reader"]["verified"]
    assert result["document_reader"]["stale_revision_denied"]
    assert result["document_reader"]["wrong_source_denied"]
