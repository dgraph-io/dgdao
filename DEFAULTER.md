# Defaulter Integration

dgdao supports populating default field values through the `Defaulter` interface. A model implements
it to stamp fields before a write, so a value that a caller left zero can still satisfy a
`validate:"required"` rule.

## The Defaulter Interface

```go
type Defaulter interface {
    ApplyDefaults(ctx context.Context) error
}
```

## Ordering: Defaults Run Before Validation

dgdao calls `ApplyDefaults` on the model before running struct validation, on these operations:

- `Insert()`
- `Upsert()`
- `Update()`
- `GetOrInsert()`

`GetAndDelete()` is a read-then-delete operation and never calls `ApplyDefaults`.

Because defaults run first, a field that `ApplyDefaults` sets can satisfy a `validate:"required"`
tag on the same field, even though the caller left it zero.

## The Set-if-Zero Contract

`ApplyDefaults` should set a field only when it is still the zero value, so it never overwrites a
value the caller explicitly provided. The one exception is a monotonic field — for example an
`UpdatedAt` timestamp — that a model may choose to set on every write regardless of its current
value.

```go
func (e *Entity) ApplyDefaults(_ context.Context) error {
    if e.Status == "" {
        e.Status = "pending"
    }
    return nil
}
```

## Usage

Implement `ApplyDefaults` on the model type; no client option is required to enable it — dgdao
detects the interface on the object passed to a write operation.

```go
type User struct {
    UID    string `json:"uid,omitempty"`
    Name   string `json:"name,omitempty" validate:"required"`
    Status string `json:"status,omitempty" validate:"required"`
}

func (u *User) ApplyDefaults(_ context.Context) error {
    if u.Status == "" {
        u.Status = "pending"
    }
    return nil
}

user := &User{Name: "Jane Doe"}
err := client.Insert(ctx, user)
// user.Status == "pending"
```

### Slices

`Insert` and `Upsert` also accept a slice of objects. dgdao invokes `ApplyDefaults` on each element
of the slice, so a batch write defaults every element rather than only the first:

```go
users := []*User{
    {Name: "Jane Doe"},
    {Name: "John Smith"},
}
err := client.Insert(ctx, users)
// users[0].Status == "pending"
// users[1].Status == "pending"
```

### No Defaults

If a model does not implement `Defaulter`, dgdao skips the hook for it entirely; the write proceeds
straight to validation as before.
