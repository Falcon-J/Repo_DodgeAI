export default function NodeDetailsCard({ nodeDetails }) {
  if (!nodeDetails) {
    return null;
  }

  const fields = Object.entries(nodeDetails.properties || {}).filter(
    ([, value]) => value !== null && value !== ""
  );

  return (
    <div className="node-details-card">
      <div className="node-details-kicker">{nodeDetails.type.replaceAll("_", " ")}</div>
      <h3>{nodeDetails.label}</h3>
      <div className="node-field">
        <strong>Node ID:</strong> {nodeDetails.id}
      </div>
      {fields.slice(0, 14).map(([key, value]) => (
        <div key={key} className="node-field">
          <strong>{key}:</strong> {String(value)}
        </div>
      ))}
    </div>
  );
}
