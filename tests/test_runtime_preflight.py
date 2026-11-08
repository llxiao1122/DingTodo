from deploy.runtime_preflight import compare


def test_runtime_comparison_detects_changed_dependency(tmp_path):
    candidate, runtime = tmp_path / 'candidate', tmp_path / 'runtime'
    for package in ('polardb_work_adapter', 'polardb_work_writer'):
        staged = candidate / 'writer_candidate' / package
        live = runtime / 'workflows' / package
        staged.mkdir(parents=True)
        live.mkdir(parents=True)
        (staged / 'module.py').write_text('same')
        (live / 'module.py').write_text('same')
    dependency = runtime / 'pymysql'
    dependency.mkdir()
    (dependency / '__init__.py').write_text('dependency')
    assert compare(candidate, runtime)['compatible']
    (runtime / 'workflows' / 'polardb_work_writer' / 'module.py').write_text('different')
    assert not compare(candidate, runtime)['compatible']


def test_snapshot_contains_only_python_and_rejects_runtime_mismatch(tmp_path):
    import pytest
    from deploy.runtime_snapshot import snapshot, verify_snapshot
    candidate, runtime = tmp_path / 'candidate', tmp_path / 'runtime'
    for package in ('polardb_work_adapter', 'polardb_work_writer'):
        staged, live = candidate / 'writer_candidate' / package, runtime / 'workflows' / package
        staged.mkdir(parents=True)
        live.mkdir(parents=True)
        (staged / '__init__.py').write_text('')
        (live / '__init__.py').write_text('')
        (live / '.env').write_text('do-not-copy')
    dependency = runtime / 'pymysql'
    dependency.mkdir()
    (dependency / '__init__.py').write_text('')
    destination = tmp_path / 'snapshot'
    result = snapshot(candidate, runtime, destination)
    assert result['files'] == 3
    verify_snapshot(destination, result['manifest_sha256'])
    assert not list(destination.rglob('.env'))
    (destination / 'pymysql' / '__init__.py').write_text('changed')
    with pytest.raises(ValueError, match='runtime_snapshot_changed'):
        verify_snapshot(destination, result['manifest_sha256'])
    (runtime / 'workflows' / 'polardb_work_writer' / '__init__.py').write_text('changed')
    with pytest.raises(ValueError, match='runtime_dependency_mismatch'):
        snapshot(candidate, runtime, tmp_path / 'other')
