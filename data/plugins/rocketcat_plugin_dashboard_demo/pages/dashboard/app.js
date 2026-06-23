(() => {
  'use strict';

  const badge = document.getElementById('bridgeBadge');
  const output = document.getElementById('contextOutput');

  async function initialize() {
    try {
      const bridge = window.RocketCatPluginDashboard;
      if (!bridge) {
        throw new Error('RocketCatPluginDashboard SDK 未注入');
      }
      await bridge.ready;
      const context = await bridge.getContext();
      badge.textContent = 'Bridge 已连接';
      badge.className = 'bridge-badge ready';
      document.getElementById('pluginName').textContent =
        context.plugin?.display_name || context.plugin?.name || '-';
      document.getElementById('pluginVersion').textContent =
        context.plugin?.version || '-';
      document.getElementById('shellVersion').textContent =
        context.version || '-';
      document.getElementById('pageName').textContent =
        context.page || '-';
      output.textContent = JSON.stringify(context, null, 2);
    } catch (error) {
      badge.textContent = 'Bridge 连接失败';
      badge.className = 'bridge-badge failed';
      output.textContent = error?.stack || error?.message || String(error);
    }
  }

  initialize();
})();
