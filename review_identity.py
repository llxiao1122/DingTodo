"""Verify that the reply identity and target belong to the active DWS user."""
from review_inbox_poll import read_dws


def validate_owner(profile: str, owner_open_id: str) -> str:
    profiles = read_dws(['profile', 'list']).get('profiles') or []
    current = [p for p in profiles if isinstance(p, dict) and p.get('isOrgCurrent') is True]
    if len(current) != 1 or current[0].get('profile') != profile:
        raise ValueError('profile_identity_mismatch')
    user_id, name = current[0].get('userId'), current[0].get('userName')
    if not isinstance(user_id, str) or not user_id or not isinstance(name, str) or not name:
        raise ValueError('owner_identity_missing')
    data = read_dws(['contact', 'user', 'search', '--query', name, '--profile', profile])
    if data.get('success') is not True or data.get('hasMore') is True or not isinstance(data.get('result'), list):
        raise ValueError('owner_identity_lookup_incomplete')
    matches = [row for row in data['result'] if isinstance(row, dict)
               and row.get('userId') == user_id and row.get('openDingTalkId') == owner_open_id]
    if len(matches) != 1:
        raise ValueError('owner_identity_mismatch')
    return user_id
