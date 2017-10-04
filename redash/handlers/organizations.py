from redash.handlers.base import BaseResource


class CurrentOrganizationResource(BaseResource):
    def get(self):
        org = self.current_org
        return {
            'name': org.name,
            'slug': org.slug,
            'google_apps_domains': org.settings.get('google_apps_domains', None),
            'background_color': org.settings.get('background-color', None),
            'color': org.settings.get('color', None),
        }
